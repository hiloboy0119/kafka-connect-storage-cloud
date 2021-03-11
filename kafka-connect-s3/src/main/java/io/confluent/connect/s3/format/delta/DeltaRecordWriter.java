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
import io.confluent.connect.s3.format.RecordView;
import io.confluent.connect.s3.format.json.JsonRecordWriterProvider;
import io.confluent.connect.storage.format.RecordWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.avro.AvroDeserializer;
import org.apache.spark.sql.avro.SchemaConverters;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer$;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder.Deserializer;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

public class DeltaRecordWriter implements RecordWriter {
  private static final Logger log = LoggerFactory.getLogger(JsonRecordWriterProvider.class);

  private final AvroData avroData;
  private final RecordView recordView;
  private final SparkSession spark;
  private final StructType destinationTableSchema;
  private final String destination;
  private final List<Row> sparkRecords = new ArrayList<>();

  private org.apache.kafka.connect.data.Schema connectSchema = null;
  private StructType sparkSchema = null;
  private AvroDeserializer avroDeserializer = null;
  private Deserializer<Row> rowDeserializer = null;

  public DeltaRecordWriter(
      AvroData avroData,
      RecordView recordView,
      SparkSession spark,
      StructType destinationTableSchema,
      String destination) {
    this.avroData = avroData;
    this.recordView = recordView;
    this.spark = spark;
    this.destinationTableSchema = destinationTableSchema;
    this.destination = destination;
  }

  @Override
  public void write(SinkRecord sinkRecord) {
    if (connectSchema == null) {
      connectSchema = recordView.getViewSchema(sinkRecord, true);
      log.warn("connectSchema: {}", connectSchema);
      org.apache.avro.Schema avroSchema = avroData.fromConnectSchema(connectSchema);
      sparkSchema = (StructType) SchemaConverters.toSqlType(avroSchema).dataType();
      avroDeserializer = new AvroDeserializer(avroSchema, sparkSchema);
      List<Attribute> attributes = JavaConverters.asJavaCollection(sparkSchema.toAttributes())
          .stream()
          .map(Attribute::toAttribute)
          .collect(Collectors.toList());
      rowDeserializer = RowEncoder.apply(sparkSchema)
          .resolveAndBind(
              JavaConverters.asScalaBuffer(attributes).toSeq(),
              SimpleAnalyzer$.MODULE$
          ).createDeserializer();
    }
    GenericRecord genericRecord = (GenericRecord) avroData.fromConnectData(
        connectSchema,
        recordView.getView(sinkRecord, true)
    );
    Row row = rowDeserializer.apply(
        (InternalRow) avroDeserializer.deserialize(genericRecord).get()
    );
    sparkRecords.add(row);
  }

  @Override
  public void close() {

  }

  @Override
  public void commit() {
    spark.createDataFrame(sparkRecords, sparkSchema).write().format("delta").save(destination);
  }
}
