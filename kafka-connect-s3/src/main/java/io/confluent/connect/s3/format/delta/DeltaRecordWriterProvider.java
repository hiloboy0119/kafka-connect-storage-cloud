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

package io.confluent.connect.s3.format.delta;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.format.RecordViewSetter;
import io.confluent.connect.s3.format.parquet.ParquetRecordWriterProvider;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.delta.tables.DeltaTable;
import java.nio.file.Paths;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeltaRecordWriterProvider  extends RecordViewSetter
    implements RecordWriterProvider<S3SinkConnectorConfig> {
  private static final Logger log = LoggerFactory.getLogger(ParquetRecordWriterProvider.class);
  private static final String EXTENSION = ".parquet";

  private final S3Storage storage;
  private final AvroData avroData;
  private final SparkSession spark;

  public DeltaRecordWriterProvider(S3Storage storage, AvroData avroData) {
    this.storage = storage;
    this.avroData = avroData;
    this.spark = SparkSession.builder()
        .appName("DeltaRecordWriter")
        .master("local[*]")
        .getOrCreate();
  }

  @Override
  public String getExtension() {
    return storage.conf().parquetCompressionCodecName().getExtension() + EXTENSION;
  }

  @Override
  public RecordWriter getRecordWriter(
      S3SinkConnectorConfig s3SinkConnectorConfig,
      String filename
  ) {
    String destinationUrl = Paths.get(storage.url())
        .resolve(storage.conf().getBucketName())
        .toString();
    log.warn("destinationUrl: {}", destinationUrl);
    DeltaTable deltaTable = DeltaTable.forPath(spark, destinationUrl);
    StructType destinationTableSchema = deltaTable.toDF().schema();
    return new DeltaRecordWriter(
        avroData,
        recordView,
        spark,
        destinationTableSchema,
        destinationUrl);
  }
}
