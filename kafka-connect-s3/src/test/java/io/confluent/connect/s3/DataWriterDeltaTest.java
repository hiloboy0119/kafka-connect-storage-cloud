/*
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.connect.s3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.amazonaws.services.s3.AmazonS3;
import io.confluent.connect.s3.format.delta.DeltaFormat;
import io.confluent.connect.s3.format.parquet.ParquetFormat;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import io.confluent.connect.storage.partitioner.Partitioner;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;

public class DataWriterDeltaTest extends TestWithMockedS3 {

  private static Path deltaDir;
  private static SparkSession spark;
  protected AmazonS3 s3;
  protected Partitioner<?> partitioner;
  protected S3Storage storage;
  protected DeltaFormat format;

  @BeforeClass
  public static void beforeClass() throws IOException {
    deltaDir = Files.createTempDirectory("spark");
    spark = SparkSession.builder()
        .appName("local_test")
        .master("local[*]")
        .getOrCreate();
    Random random = new Random();
    List<Row> data = random.doubles(1000)
        .boxed()
        .map(RowFactory::create)
        .collect(Collectors.toList());

    List<StructField> fields = new ArrayList<>();
    fields.add(DataTypes.createStructField("number", DataTypes.DoubleType, true));
    spark.createDataFrame(data, DataTypes.createStructType(fields))
        .write()
        .format("delta")
        .save(deltaDir.toAbsolutePath().toString());
  }

  @AfterClass
  public static void afterClass() throws IOException {
    FileUtils.deleteDirectory(deltaDir.toFile());
    spark.stop();
  }

  @Override
  protected Map<String, String> createProps() {
    Map<String, String> props = super.createProps();
    props.put(S3SinkConnectorConfig.S3_BUCKET_CONFIG, deltaDir.toString());
    props.put(S3SinkConnectorConfig.FORMAT_CLASS_CONFIG, ParquetFormat.class.getName());
    return props;
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();

    s3 = PowerMockito.spy(newS3Client(connectorConfig));
    storage = new S3Storage(connectorConfig, "file:///", deltaDir.toString(), s3);

    partitioner = new DefaultPartitioner<>();
    partitioner.configure(parsedConfig);

    s3.createBucket(S3_TEST_BUCKET_NAME);
    @SuppressWarnings("deprecation")
    boolean bucketExists = s3.doesBucketExist(S3_TEST_BUCKET_NAME);
    assertTrue(bucketExists);

    format = new DeltaFormat(storage);
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @Test
  public void writeTest() throws Exception {
    long deltaCountBefore = spark.read().format("delta").load(deltaDir.toAbsolutePath().toString()).count();
    assertEquals(1000, deltaCountBefore);

    S3SinkTask task = new S3SinkTask(connectorConfig, context, storage, partitioner, format, SYSTEM_TIME);

    DataWriterParquetTest recordMaker = new DataWriterParquetTest();

    List<SinkRecord> sinkRecords = recordMaker.createRecords(7, 0, context.assignment());
    assertEquals(14, sinkRecords.size());
    // Perform write
    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    long deltaCountAfter = spark.read().format("delta").load(deltaDir.toAbsolutePath().toString()).count();
    //two commits of 3 records each for 2 partitions
    Assert.assertEquals(1012, deltaCountAfter);
  }

}
