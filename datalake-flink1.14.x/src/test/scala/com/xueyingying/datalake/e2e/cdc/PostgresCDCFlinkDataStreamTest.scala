package com.xueyingying.datalake.e2e.cdc

import java.util.concurrent.TimeUnit
import java.util.{Properties, UUID}

import com.xueyingying.datalake.FlinkSuiteBase
import com.xueyingying.datalake.common.event.Event
import com.xueyingying.datalake.converters.pg.{DateToStringConverter, DecimalToStringConverter, TimeToStringConverter, TimestampToStringConverter}
import com.xueyingying.datalake.debezium.event.{DebeziumEventDeserializationSchema, DebeziumSchemaDataTypeInference}
import com.ververica.cdc.connectors.base.options.StartupOptions
import com.ververica.cdc.connectors.postgres.PostgreSQLSource
import com.ververica.cdc.connectors.postgres.source.PostgresSourceBuilder
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema
import io.debezium.config.CommonConnectorConfig
import io.debezium.connector.postgresql.PostgresConnectorConfig
import io.debezium.connector.postgresql.PostgresConnectorConfig.{AutoCreateMode, SnapshotMode}
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.util.Collector
import org.apache.kafka.clients.producer.ProducerConfig

import scala.collection.JavaConversions.propertiesAsScalaMap

/**
 * @author cx330.1000ly@gmail.com
 * @version 1.0.0
 * @since 2023-01-19
 */
class PostgresCDCFlinkDataStreamTest extends FlinkSuiteBase {
  "pg legacy streaming connector" should "work" in {
    val prop = new Properties
    prop += (CommonConnectorConfig.CUSTOM_CONVERTERS.name -> "a,b,c,d")
    prop += ("a.type" -> classOf[TimestampToStringConverter].getCanonicalName)
    prop += ("b.type" -> classOf[TimeToStringConverter].getCanonicalName)
    prop += ("c.type" -> classOf[DecimalToStringConverter].getCanonicalName)
    prop += ("d.type" -> classOf[DateToStringConverter].getCanonicalName)
    prop += (PostgresConnectorConfig.SNAPSHOT_MODE.name -> SnapshotMode.NEVER.getValue)

    val pgSource = PostgreSQLSource.builder[Event]
      .hostname("localhost")
      .port(5432)
      .database("cdc_test")
      .schemaList("s1")
      .tableList("s1.t1")
      .username("postgres")
      .password("postgres")
      .deserializer(new DebeziumEventDeserializationSchema(new DebeziumSchemaDataTypeInference()))
      .slotName("test")
      .decodingPluginName("pgoutput")
      .debeziumProperties(prop)
      .build

    val properties = new Properties
    properties += (ProducerConfig.TRANSACTION_TIMEOUT_CONFIG -> TimeUnit.MINUTES.toMillis(15).toString)
    properties += (("security.protocol", "SASL_PLAINTEXT"))
    properties += ("sasl.mechanism" -> "PLAIN")

    val kafkaSink = KafkaSink.builder[String]
      .setBootstrapServers("localhost:9092")
      .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
      .setRecordSerializer(KafkaRecordSerializationSchema.builder[String]
        .setValueSerializationSchema(new SimpleStringSchema)
        .setTopic("test")
        .build
      )
      .setTransactionalIdPrefix(UUID.randomUUID.toString)
      .setKafkaProducerConfig(properties)
      .build
    val process = env.addSource(pgSource).slotSharingGroup("Postgres Source").rebalance
      .filter(_ => true)

    // process.sinkTo(kafkaSink)
    process.print

    env.execute
  }

  "pg parallel streaming connector" should "work" in {
    val prop = new Properties
    prop += (CommonConnectorConfig.CUSTOM_CONVERTERS.name -> "a,b,c,d")
    prop += ("a.type" -> classOf[TimestampToStringConverter].getCanonicalName)
    prop += ("b.type" -> classOf[TimeToStringConverter].getCanonicalName)
    prop += ("c.type" -> classOf[DecimalToStringConverter].getCanonicalName)
    prop += ("d.type" -> classOf[DateToStringConverter].getCanonicalName)
    prop += (PostgresConnectorConfig.PUBLICATION_AUTOCREATE_MODE.name -> AutoCreateMode.ALL_TABLES.getValue)
    // prop += (PostgresConnectorConfig.PUBLICATION_NAME.name -> "test")

    val pgSource = PostgresSourceBuilder.PostgresIncrementalSource.builder[String]
      .hostname("localhost")
      .port(5432)
      .database("cdc_test")
      .schemaList("s1")
      .tableList("s1.t1")
      .username("test")
      .password("test")
      .deserializer(new JsonDebeziumDeserializationSchema)
      .slotName("test")
      .decodingPluginName("pgoutput")
      .debeziumProperties(prop)
      // .splitSize(999999)
      .startupOptions(StartupOptions.latest())
      .build

    env.fromSource(pgSource, WatermarkStrategy.noWatermarks[String], "PostgresParallelSource").slotSharingGroup("1").disableChaining().rebalance.print
    env.execute("Print Postgres Snapshot + WAL")
  }
}
