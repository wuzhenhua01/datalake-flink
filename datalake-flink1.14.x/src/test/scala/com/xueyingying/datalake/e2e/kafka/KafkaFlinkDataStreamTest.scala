package com.xueyingying.datalake.e2e.kafka

import java.util.concurrent.TimeUnit
import java.util.{Properties, UUID}

import com.xueyingying.datalake.FlinkSuiteBase
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink, TopicSelector}
import org.apache.flink.formats.raw.RawFormatSerializationSchema
import org.apache.flink.streaming.api.scala.{DataStream, createTypeInformation}
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner
import org.apache.flink.table.data.RowData
import org.apache.flink.types.Row
import org.apache.kafka.clients.producer.ProducerConfig

import scala.collection.JavaConversions.propertiesAsScalaMap

/**
 * @author cx330.1000ly@gmail.com
 * @version 1.0.0
 * @since 2023-09-14
 */
class KafkaFlinkDataStreamTest extends FlinkSuiteBase {
  "kafka" should "write" in {
    val tableResult = tabEnv.sqlQuery("SELECT 'test', 0, content FROM datagen")
    val rowStream = tabEnv.toDataStream(tableResult).map {
      row => (row.getField(0).asInstanceOf[String], row.getField(1).asInstanceOf[Int].toLong, row.getField("content").asInstanceOf[String])
    }

    val properties = new Properties
    properties += (ProducerConfig.TRANSACTION_TIMEOUT_CONFIG -> TimeUnit.MINUTES.toMillis(15).toString)
    properties += (("security.protocol", "SASL_PLAINTEXT"))
    properties += ("sasl.mechanism" -> "PLAIN")
    // properties += ("sasl.jaas.config" -> "org.apache.kafka.common.security.plain.PlainLoginModule required username='xiaowu' password='xiaowu';")

    val serializationSchema = new SerializationSchema[(String, Long, String)] {
      override def serialize(data: (String, Long, String)): Array[Byte] = data._3.getBytes
    }

    val topicSelector = new TopicSelector[(String, Long, String)] {
      override def apply(data: (String, Long, String)): String = data._1
    }

    val partitioner = new FlinkKafkaPartitioner[(String, Long, String)] {
      override def partition(record: (String, Long, String), key: Array[Byte], value: Array[Byte], targetTopic: String, partitions: Array[Int]): Int = record._2.toInt
    }

    val kafkaSink = KafkaSink.builder[(String, Long, String)]
      .setBootstrapServers("localhost:9095")
      .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
      .setRecordSerializer(KafkaRecordSerializationSchema.builder[(String, Long, String)]
        .setValueSerializationSchema(serializationSchema)
        .setTopicSelector(topicSelector)
        .setPartitioner(partitioner)
        .build
      )
      .setTransactionalIdPrefix(UUID.randomUUID.toString)
      .setKafkaProducerConfig(properties)
      .build

    rowStream.sinkTo(kafkaSink)
    env.execute
  }

  "kafka" should "read" in {

  }
}
