/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigtable.testproxy;

import static com.google.common.truth.Truth.assertThat;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.bigtable.v2.ArrayValue;
import com.google.bigtable.v2.BigtableGrpc;
import com.google.bigtable.v2.PartialRowResponse;
import com.google.bigtable.v2.TableSchema;
import com.google.bigtable.v2.Type;
import com.google.bigtable.v2.TypedCell;
import com.google.bigtable.v2.TypedColumn;
import com.google.bigtable.v2.TypedFamily;
import com.google.bigtable.v2.TypedReadRowsResponse;
import com.google.bigtable.v2.TypedRow;
import com.google.bigtable.v2.TypedRows;
import com.google.bigtable.v2.TypedRowsBatch;
import com.google.bigtable.v2.Value;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.common.hash.Hashing;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.rpc.Code;
import com.google.type.Date;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CbtTestProxyTypedReadRowsTest {

  private static final String CLIENT_ID = "test-client";
  private static final String TABLE_NAME = "projects/p/instances/i/tables/t";

  private Server inProcessServer;
  private ManagedChannel inProcessChannel;
  private CbtTestProxy testProxy;
  private MockBigtableService mockBigtableService;

  @Before
  @SuppressWarnings("deprecation")
  public void setUp() throws IOException {
    String serverName = InProcessServerBuilder.generateName();
    mockBigtableService = new MockBigtableService();
    inProcessServer =
        InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(mockBigtableService)
            .build()
            .start();

    inProcessChannel = InProcessChannelBuilder.forName(serverName).directExecutor().build();

    testProxy = CbtTestProxy.create();

    BigtableDataSettings.Builder settingsBuilder =
        BigtableDataSettings.newBuilder()
            .setRefreshingChannel(false)
            .setProjectId("p")
            .setInstanceId("i")
            .setCredentialsProvider(NoCredentialsProvider.create());
    settingsBuilder
        .stubSettings()
        .setTransportChannelProvider(
            FixedTransportChannelProvider.create(GrpcTransportChannel.create(inProcessChannel)));
    settingsBuilder
        .stubSettings()
        .readRowsSettings()
        .retrySettings()
        .setInitialRetryDelayDuration(Duration.ofMillis(1))
        .setMaxRetryDelayDuration(Duration.ofMillis(10));
    BigtableDataSettings settings = settingsBuilder.build();
    BigtableDataClient dataClient = BigtableDataClient.create(settings);
    CbtTestProxy.CbtClient client = CbtTestProxy.CbtClient.create(settings, dataClient);
    testProxy.registerClientForTest(CLIENT_ID, client);
  }

  @After
  public void tearDown() throws InterruptedException {
    testProxy.close();
    inProcessChannel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    inProcessServer.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
  }

  private static TypedRow createRow(String rowKey, String family, String col, String val) {
    return TypedRow.newBuilder()
        .setRowKey(Value.newBuilder().setRawValue(ByteString.copyFromUtf8(rowKey)).build())
        .addFamilies(
            TypedFamily.newBuilder()
                .setFamilyName(family)
                .addColumns(
                    TypedColumn.newBuilder()
                        .setQualifier(
                            Value.newBuilder().setRawValue(ByteString.copyFromUtf8(col)).build())
                        .addCells(
                            TypedCell.newBuilder()
                                .setValue(
                                    Value.newBuilder()
                                        .setRawValue(ByteString.copyFromUtf8(val))
                                        .build())
                                .setTimestamp(
                                    Timestamp.newBuilder()
                                        .setSeconds(12345)
                                        .setNanos(678000)
                                        .build())
                                .addLabels("lbl-1")
                                .build())
                        .build())
                .build())
        .build();
  }

  private static int computeBatchChecksum(ByteString batchBytes, int prevChecksum) {
    return Hashing.crc32c()
        .newHasher()
        .putBytes(batchBytes.toByteArray())
        .putInt(prevChecksum)
        .hash()
        .asInt();
  }

  private static TypedReadRowsRequest defaultTableRequest() {
    return TypedReadRowsRequest.newBuilder()
        .setClientId(CLIENT_ID)
        .setRequest(
            com.google.bigtable.v2.TypedReadRowsRequest.newBuilder()
                .setTableName(TABLE_NAME)
                .build())
        .build();
  }

  private TypedRowsResult executeTypedReadRows(TypedReadRowsRequest request) throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    List<TypedRowsResult> results = new ArrayList<>();
    AtomicReference<Throwable> error = new AtomicReference<>();
    testProxy.typedReadRows(
        request,
        new StreamObserver<TypedRowsResult>() {
          @Override
          public void onNext(TypedRowsResult value) {
            results.add(value);
          }

          @Override
          public void onError(Throwable t) {
            error.set(t);
            latch.countDown();
          }

          @Override
          public void onCompleted() {
            latch.countDown();
          }
        });

    assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(error.get()).isNull();
    assertThat(results).hasSize(1);
    return results.get(0);
  }

  @Test
  public void testTypedReadRows_successfulStream() throws Exception {
    TypedRow row1 = createRow("rk-1", "cf1", "cq1", "val1");
    TypedRow row2 = createRow("rk-2", "cf1", "cq2", "val2");
    TypedRows batch = TypedRows.newBuilder().addRows(row1).addRows(row2).build();
    ByteString batchBytes = batch.toByteString();
    int checksum = computeBatchChecksum(batchBytes, 0);

    mockBigtableService.responses.add(
        TypedReadRowsResponse.newBuilder()
            .setTableSchema(TableSchema.getDefaultInstance())
            .setResponse(
                PartialRowResponse.newBuilder()
                    .setTypedRowsBatch(TypedRowsBatch.newBuilder().setBatchData(batchBytes).build())
                    .setFlush(
                        PartialRowResponse.Flush.newBuilder()
                            .setChecksum(checksum)
                            .setResumeToken(ByteString.copyFromUtf8("token-1"))
                            .build())
                    .build())
            .build());

    TypedRowsResult result = executeTypedReadRows(defaultTableRequest());
    assertThat(result.getStatus().getCode()).isEqualTo(Code.OK_VALUE);
    assertThat(result.getRowsList()).containsExactly(row1, row2).inOrder();
  }

  @Test
  public void testTypedReadRows_structuredRowKeys() throws Exception {
    TableSchema structuredSchema =
        TableSchema.newBuilder()
            .setRowKeySchema(
                Type.Struct.newBuilder()
                    .addFields(
                        Type.Struct.Field.newBuilder()
                            .setFieldName("s")
                            .setType(
                                Type.newBuilder().setStringType(Type.String.getDefaultInstance())))
                    .addFields(
                        Type.Struct.Field.newBuilder()
                            .setFieldName("b")
                            .setType(
                                Type.newBuilder().setBytesType(Type.Bytes.getDefaultInstance())))
                    .addFields(
                        Type.Struct.Field.newBuilder()
                            .setFieldName("i")
                            .setType(
                                Type.newBuilder().setInt64Type(Type.Int64.getDefaultInstance())))
                    .addFields(
                        Type.Struct.Field.newBuilder()
                            .setFieldName("f64")
                            .setType(
                                Type.newBuilder()
                                    .setFloat64Type(Type.Float64.getDefaultInstance())))
                    .addFields(
                        Type.Struct.Field.newBuilder()
                            .setFieldName("f32")
                            .setType(
                                Type.newBuilder()
                                    .setFloat32Type(Type.Float32.getDefaultInstance())))
                    .addFields(
                        Type.Struct.Field.newBuilder()
                            .setFieldName("bool")
                            .setType(Type.newBuilder().setBoolType(Type.Bool.getDefaultInstance())))
                    .addFields(
                        Type.Struct.Field.newBuilder()
                            .setFieldName("ts")
                            .setType(
                                Type.newBuilder()
                                    .setTimestampType(Type.Timestamp.getDefaultInstance())))
                    .addFields(
                        Type.Struct.Field.newBuilder()
                            .setFieldName("dt")
                            .setType(Type.newBuilder().setDateType(Type.Date.getDefaultInstance())))
                    .addFields(
                        Type.Struct.Field.newBuilder()
                            .setFieldName("nullable")
                            .setType(
                                Type.newBuilder().setStringType(Type.String.getDefaultInstance()))))
            .build();

    TypedRow structuredRow =
        createRow("unused", "cf1", "cq1", "val1").toBuilder()
            .setRowKey(
                Value.newBuilder()
                    .setArrayValue(
                        ArrayValue.newBuilder()
                            .addValues(Value.newBuilder().setStringValue("tenant-a"))
                            .addValues(
                                Value.newBuilder()
                                    .setBytesValue(ByteString.copyFrom(new byte[] {0x01, 0x02})))
                            .addValues(Value.newBuilder().setIntValue(42L))
                            .addValues(Value.newBuilder().setFloatValue(3.141592653589793))
                            .addValues(Value.newBuilder().setFloatValue(1.5))
                            .addValues(Value.newBuilder().setBoolValue(true))
                            .addValues(
                                Value.newBuilder()
                                    .setTimestampValue(
                                        Timestamp.newBuilder()
                                            .setSeconds(1700000000L)
                                            .setNanos(123456000)))
                            .addValues(
                                Value.newBuilder()
                                    .setDateValue(
                                        Date.newBuilder().setYear(2026).setMonth(3).setDay(25)))
                            .addValues(Value.getDefaultInstance())))
            .build();

    ByteString batchBytes = TypedRows.newBuilder().addRows(structuredRow).build().toByteString();
    int checksum = computeBatchChecksum(batchBytes, 0);

    mockBigtableService.responses.add(
        TypedReadRowsResponse.newBuilder()
            .setTableSchema(structuredSchema)
            .setResponse(
                PartialRowResponse.newBuilder()
                    .setTypedRowsBatch(TypedRowsBatch.newBuilder().setBatchData(batchBytes).build())
                    .setFlush(
                        PartialRowResponse.Flush.newBuilder()
                            .setChecksum(checksum)
                            .setResumeToken(ByteString.copyFromUtf8("token-srk"))
                            .build())
                    .build())
            .build());

    TypedRowsResult result = executeTypedReadRows(defaultTableRequest());
    assertThat(result.getStatus().getCode()).isEqualTo(Code.OK_VALUE);
    assertThat(result.getRowsList()).containsExactly(structuredRow);
  }

  @Test
  public void testTypedReadRows_fragmentedBatchReassembly() throws Exception {
    TypedRow row = createRow("rk-frag", "cf", "col", "value");
    TypedRows batch = TypedRows.newBuilder().addRows(row).build();
    ByteString batchBytes = batch.toByteString();
    int checksum = computeBatchChecksum(batchBytes, 0);

    int mid = batchBytes.size() / 2;
    ByteString part1 = batchBytes.substring(0, mid);
    ByteString part2 = batchBytes.substring(mid);

    mockBigtableService.responses.add(
        TypedReadRowsResponse.newBuilder()
            .setTableSchema(TableSchema.getDefaultInstance())
            .setResponse(
                PartialRowResponse.newBuilder()
                    .setTypedRowsBatch(TypedRowsBatch.newBuilder().setBatchData(part1).build())
                    .build())
            .build());

    mockBigtableService.responses.add(
        TypedReadRowsResponse.newBuilder()
            .setResponse(
                PartialRowResponse.newBuilder()
                    .setTypedRowsBatch(TypedRowsBatch.newBuilder().setBatchData(part2).build())
                    .setFlush(
                        PartialRowResponse.Flush.newBuilder()
                            .setChecksum(checksum)
                            .setResumeToken(ByteString.copyFromUtf8("token-frag"))
                            .build())
                    .build())
            .build());

    TypedRowsResult result = executeTypedReadRows(defaultTableRequest());
    assertThat(result.getStatus().getCode()).isEqualTo(Code.OK_VALUE);
    assertThat(result.getRowsCount()).isEqualTo(1);
    assertThat(result.getRows(0).getRowKey().getRawValue().toStringUtf8()).isEqualTo("rk-frag");
  }

  @Test
  public void testTypedReadRows_resetDiscardsBufferedData() throws Exception {
    TypedRow row = createRow("rk-valid", "cf", "col", "val");
    TypedRows batch = TypedRows.newBuilder().addRows(row).build();
    ByteString batchBytes = batch.toByteString();
    int checksum = computeBatchChecksum(batchBytes, 0);

    // 1. Partial response with junk bytes
    mockBigtableService.responses.add(
        TypedReadRowsResponse.newBuilder()
            .setTableSchema(TableSchema.getDefaultInstance())
            .setResponse(
                PartialRowResponse.newBuilder()
                    .setTypedRowsBatch(
                        TypedRowsBatch.newBuilder()
                            .setBatchData(ByteString.copyFromUtf8("invalid_bytes"))
                            .build())
                    .build())
            .build());

    // 2. Response with reset=true followed by valid batch and flush
    mockBigtableService.responses.add(
        TypedReadRowsResponse.newBuilder()
            .setResponse(
                PartialRowResponse.newBuilder()
                    .setReset(true)
                    .setTypedRowsBatch(TypedRowsBatch.newBuilder().setBatchData(batchBytes).build())
                    .setFlush(
                        PartialRowResponse.Flush.newBuilder()
                            .setChecksum(checksum)
                            .setResumeToken(ByteString.copyFromUtf8("token-valid"))
                            .build())
                    .build())
            .build());

    TypedRowsResult result = executeTypedReadRows(defaultTableRequest());
    assertThat(result.getStatus().getCode()).isEqualTo(Code.OK_VALUE);
    assertThat(result.getRowsCount()).isEqualTo(1);
    assertThat(result.getRows(0).getRowKey().getRawValue().toStringUtf8()).isEqualTo("rk-valid");
  }

  @Test
  public void testTypedReadRows_checksumMismatchReturnsUnavailable() throws Exception {
    TypedRow row = createRow("rk-bad-crc", "cf", "col", "val");
    ByteString batchBytes = TypedRows.newBuilder().addRows(row).build().toByteString();
    int badChecksum = computeBatchChecksum(batchBytes, 0) ^ 0xFFFFFFFF;

    mockBigtableService.responses.add(
        TypedReadRowsResponse.newBuilder()
            .setTableSchema(TableSchema.getDefaultInstance())
            .setResponse(
                PartialRowResponse.newBuilder()
                    .setTypedRowsBatch(TypedRowsBatch.newBuilder().setBatchData(batchBytes).build())
                    .setFlush(
                        PartialRowResponse.Flush.newBuilder()
                            .setChecksum(badChecksum)
                            .setResumeToken(ByteString.copyFromUtf8("token-bad"))
                            .build())
                    .build())
            .build());

    TypedRowsResult result = executeTypedReadRows(defaultTableRequest());
    assertThat(result.getStatus().getCode()).isEqualTo(Code.UNAVAILABLE_VALUE);
    assertThat(result.getStatus().getMessage()).contains("Checksum mismatch");
  }

  @Test
  public void testTypedReadRows_cancelAfterRowsStopsStream() throws Exception {
    mockBigtableService.keepStreamOpen = true;
    int runningChecksum = 0;
    for (int i = 1; i <= 5; i++) {
      TypedRow row = createRow("rk-" + i, "cf", "col", "val");
      TypedRows batch = TypedRows.newBuilder().addRows(row).build();
      ByteString batchBytes = batch.toByteString();
      runningChecksum = computeBatchChecksum(batchBytes, runningChecksum);

      TypedReadRowsResponse.Builder respBuilder = TypedReadRowsResponse.newBuilder();
      if (i == 1) {
        respBuilder.setTableSchema(TableSchema.getDefaultInstance());
      }
      respBuilder.setResponse(
          PartialRowResponse.newBuilder()
              .setTypedRowsBatch(TypedRowsBatch.newBuilder().setBatchData(batchBytes).build())
              .setFlush(
                  PartialRowResponse.Flush.newBuilder()
                      .setChecksum(runningChecksum)
                      .setResumeToken(ByteString.copyFromUtf8("tok-" + i))
                      .build())
              .build());
      mockBigtableService.responses.add(respBuilder.build());
    }

    TypedReadRowsRequest request = defaultTableRequest().toBuilder().setCancelAfterRows(2).build();

    TypedRowsResult result = executeTypedReadRows(request);
    assertThat(result.getStatus().getCode()).isEqualTo(Code.OK_VALUE);
    assertThat(result.getRowsCount()).isEqualTo(2);
    assertThat(result.getRows(0).getRowKey().getRawValue().toStringUtf8()).isEqualTo("rk-1");
    assertThat(result.getRows(1).getRowKey().getRawValue().toStringUtf8()).isEqualTo("rk-2");
    assertThat(mockBigtableService.streamCancelled.get()).isTrue();
  }

  @Test
  public void testTypedReadRows_serverErrorPropagatedInStatus() throws Exception {
    mockBigtableService.errorToThrow =
        Status.NOT_FOUND.withDescription("Table does not exist").asRuntimeException();

    TypedRowsResult result = executeTypedReadRows(defaultTableRequest());
    assertThat(result.getStatus().getCode()).isEqualTo(Code.NOT_FOUND_VALUE);
    assertThat(result.getStatus().getMessage()).contains("Table does not exist");
  }

  @Test
  public void testTypedReadRows_missingTargetReturnsInvalidArgument() throws Exception {
    TypedReadRowsRequest request =
        TypedReadRowsRequest.newBuilder()
            .setClientId(CLIENT_ID)
            .setRequest(com.google.bigtable.v2.TypedReadRowsRequest.getDefaultInstance())
            .build();

    TypedRowsResult result = executeTypedReadRows(request);
    assertThat(result.getStatus().getCode()).isEqualTo(Code.INVALID_ARGUMENT_VALUE);
  }

  @Test
  public void testTypedReadRows_multiBatchRunningCrc32c() throws Exception {
    TypedRow row1 = createRow("rk-1", "cf", "col", "val1");
    TypedRows batch1 = TypedRows.newBuilder().addRows(row1).build();
    ByteString batchBytes1 = batch1.toByteString();
    int checksum1 = computeBatchChecksum(batchBytes1, 0);

    TypedRow row2 = createRow("rk-2", "cf", "col", "val2");
    TypedRows batch2 = TypedRows.newBuilder().addRows(row2).build();
    ByteString batchBytes2 = batch2.toByteString();
    int checksum2 = computeBatchChecksum(batchBytes2, checksum1);

    mockBigtableService.responses.add(
        TypedReadRowsResponse.newBuilder()
            .setTableSchema(TableSchema.getDefaultInstance())
            .setResponse(
                PartialRowResponse.newBuilder()
                    .setTypedRowsBatch(
                        TypedRowsBatch.newBuilder().setBatchData(batchBytes1).build())
                    .setFlush(
                        PartialRowResponse.Flush.newBuilder()
                            .setChecksum(checksum1)
                            .setResumeToken(ByteString.copyFromUtf8("tok-1"))
                            .build())
                    .build())
            .build());

    mockBigtableService.responses.add(
        TypedReadRowsResponse.newBuilder()
            .setResponse(
                PartialRowResponse.newBuilder()
                    .setTypedRowsBatch(
                        TypedRowsBatch.newBuilder().setBatchData(batchBytes2).build())
                    .setFlush(
                        PartialRowResponse.Flush.newBuilder()
                            .setChecksum(checksum2)
                            .setResumeToken(ByteString.copyFromUtf8("tok-2"))
                            .build())
                    .build())
            .build());

    TypedRowsResult result = executeTypedReadRows(defaultTableRequest());
    assertThat(result.getStatus().getCode()).isEqualTo(Code.OK_VALUE);
    assertThat(result.getRowsCount()).isEqualTo(2);
    assertThat(result.getRows(0).getRowKey().getRawValue().toStringUtf8()).isEqualTo("rk-1");
    assertThat(result.getRows(1).getRowKey().getRawValue().toStringUtf8()).isEqualTo("rk-2");
  }

  @Test
  public void testTypedReadRows_resetRollsBackToLastResumeToken() throws Exception {
    // 1. Batch 1 committed with resume token "tok-1"
    TypedRow row1 = createRow("rk-committed", "cf", "col", "val1");
    TypedRows batch1 = TypedRows.newBuilder().addRows(row1).build();
    ByteString batchBytes1 = batch1.toByteString();
    int checksum1 = computeBatchChecksum(batchBytes1, 0);

    // 2. Batch 2 buffered (unflushed / uncommitted)
    TypedRow row2 = createRow("rk-uncommitted", "cf", "col", "val2");
    TypedRows batch2 = TypedRows.newBuilder().addRows(row2).build();
    ByteString batchBytes2 = batch2.toByteString();

    // 3. Reset occurs, discarding batch 2, followed by Batch 3 with resume token "tok-3"
    TypedRow row3 = createRow("rk-after-reset", "cf", "col", "val3");
    TypedRows batch3 = TypedRows.newBuilder().addRows(row3).build();
    ByteString batchBytes3 = batch3.toByteString();
    int checksum3 = computeBatchChecksum(batchBytes3, checksum1);

    mockBigtableService.responses.add(
        TypedReadRowsResponse.newBuilder()
            .setTableSchema(TableSchema.getDefaultInstance())
            .setResponse(
                PartialRowResponse.newBuilder()
                    .setTypedRowsBatch(
                        TypedRowsBatch.newBuilder().setBatchData(batchBytes1).build())
                    .setFlush(
                        PartialRowResponse.Flush.newBuilder()
                            .setChecksum(checksum1)
                            .setResumeToken(ByteString.copyFromUtf8("tok-1"))
                            .build())
                    .build())
            .build());

    mockBigtableService.responses.add(
        TypedReadRowsResponse.newBuilder()
            .setResponse(
                PartialRowResponse.newBuilder()
                    .setTypedRowsBatch(
                        TypedRowsBatch.newBuilder().setBatchData(batchBytes2).build())
                    .build())
            .build());

    mockBigtableService.responses.add(
        TypedReadRowsResponse.newBuilder()
            .setResponse(
                PartialRowResponse.newBuilder()
                    .setReset(true)
                    .setTypedRowsBatch(
                        TypedRowsBatch.newBuilder().setBatchData(batchBytes3).build())
                    .setFlush(
                        PartialRowResponse.Flush.newBuilder()
                            .setChecksum(checksum3)
                            .setResumeToken(ByteString.copyFromUtf8("tok-3"))
                            .build())
                    .build())
            .build());

    TypedRowsResult result = executeTypedReadRows(defaultTableRequest());
    assertThat(result.getStatus().getCode()).isEqualTo(Code.OK_VALUE);
    // row2 must have been discarded on reset, leaving only row1 and row3
    assertThat(result.getRowsCount()).isEqualTo(2);
    assertThat(result.getRows(0).getRowKey().getRawValue().toStringUtf8())
        .isEqualTo("rk-committed");
    assertThat(result.getRows(1).getRowKey().getRawValue().toStringUtf8())
        .isEqualTo("rk-after-reset");
  }

  @Test
  public void testTypedReadRows_midStreamErrorPreservesCommittedRows() throws Exception {
    TypedRow row1 = createRow("rk-committed", "cf", "col", "val1");
    TypedRows batch1 = TypedRows.newBuilder().addRows(row1).build();
    ByteString batchBytes1 = batch1.toByteString();
    int checksum1 = computeBatchChecksum(batchBytes1, 0);

    mockBigtableService.responses.add(
        TypedReadRowsResponse.newBuilder()
            .setTableSchema(TableSchema.getDefaultInstance())
            .setResponse(
                PartialRowResponse.newBuilder()
                    .setTypedRowsBatch(
                        TypedRowsBatch.newBuilder().setBatchData(batchBytes1).build())
                    .setFlush(
                        PartialRowResponse.Flush.newBuilder()
                            .setChecksum(checksum1)
                            .setResumeToken(ByteString.copyFromUtf8("tok-1"))
                            .build())
                    .build())
            .build());
    mockBigtableService.errorToThrow =
        Status.PERMISSION_DENIED.withDescription("Access revoked mid-stream").asRuntimeException();

    TypedRowsResult result = executeTypedReadRows(defaultTableRequest());
    assertThat(result.getStatus().getCode()).isEqualTo(Code.PERMISSION_DENIED_VALUE);
    assertThat(result.getStatus().getMessage()).contains("Access revoked mid-stream");
    assertThat(result.getRowsList()).containsExactly(row1);
  }

  @Test
  public void testTypedReadRows_unknownClientIdReturnsNotFoundOnObserver() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    List<TypedRowsResult> results = new ArrayList<>();
    AtomicReference<Throwable> error = new AtomicReference<>();
    TypedReadRowsRequest request =
        defaultTableRequest().toBuilder().setClientId("unknown-client-id").build();

    testProxy.typedReadRows(
        request,
        new StreamObserver<TypedRowsResult>() {
          @Override
          public void onNext(TypedRowsResult value) {
            results.add(value);
          }

          @Override
          public void onError(Throwable t) {
            error.set(t);
            latch.countDown();
          }

          @Override
          public void onCompleted() {
            latch.countDown();
          }
        });

    assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(results).isEmpty();
    assertThat(error.get()).isNotNull();
    assertThat(Status.fromThrowable(error.get()).getCode()).isEqualTo(Status.Code.NOT_FOUND);
  }

  private static class MockBigtableService extends BigtableGrpc.BigtableImplBase {
    final List<TypedReadRowsResponse> responses = new ArrayList<>();
    final AtomicBoolean streamCancelled = new AtomicBoolean(false);
    RuntimeException errorToThrow = null;
    boolean keepStreamOpen = false;

    @Override
    public void typedReadRows(
        com.google.bigtable.v2.TypedReadRowsRequest request,
        StreamObserver<TypedReadRowsResponse> responseObserver) {
      if (responseObserver instanceof ServerCallStreamObserver) {
        ((ServerCallStreamObserver<TypedReadRowsResponse>) responseObserver)
            .setOnCancelHandler(() -> streamCancelled.set(true));
      }
      for (TypedReadRowsResponse resp : responses) {
        responseObserver.onNext(resp);
      }
      if (errorToThrow != null) {
        responseObserver.onError(errorToThrow);
        return;
      }
      if (!keepStreamOpen) {
        responseObserver.onCompleted();
      }
    }
  }
}
