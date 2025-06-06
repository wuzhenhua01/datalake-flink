package com.xueyingying.datalake.e2e.cdc

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, SerializationFeature}
import com.ververica.cdc.connectors.mysql.source.MySqlSource
import com.ververica.cdc.connectors.mysql.table.StartupOptions
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema
import com.xueyingying.datalake.FlinkSuiteBase
import io.debezium.data.Envelope
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.state.{MapStateDescriptor, ReadOnlyBroadcastState}
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, createTypeInformation}
import org.apache.flink.util.Collector

import scala.collection.JavaConversions.iterableAsScalaIterable

/**
 * @author cx330.1000ly@gmail.com
 * @version 1.0.0
 * @since 2023-10-12
 */
class MySQLCDCFlinkDataStreamTest extends FlinkSuiteBase {
  val mapper = new ObjectMapper
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  mapper.configure(SerializationFeature.INDENT_OUTPUT, false)

  "mysql streaming connector" should "work" in {
    val dataStream: DataStream[(Long, String)] = rowStream.map(row => (row.getField(0).asInstanceOf[Long], row.getField(1).asInstanceOf[String]))

    val mysqlSource = MySqlSource.builder()
      .hostname("xueyingying.com")
      .port(3306)
      .username("test")
      .password("test")
      .databaseList("demo")
      .tableList("demo.t2")
      .deserializer(new JsonDebeziumDeserializationSchema)
      .closeIdleReaders(true)
      .startupOptions(StartupOptions.initial())
      .build()
    val cellKeyData = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "mysql").map(mapper.readValue(_, classOf[Map[String, Any]]))
    val cellKeyStateDescriptor = new MapStateDescriptor[String, String]("test", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO)
    val cellKeyBroadcast = cellKeyData.broadcast(cellKeyStateDescriptor)

    dataStream.connect(cellKeyBroadcast).process(new BroadcastProcessFunction[(Long, String), Map[String, _], String]() {
      val cellKeyStateDescriptor = new MapStateDescriptor[String, String]("test", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO)

      override def processElement(value: (Long, String), ctx: BroadcastProcessFunction[(Long, String), Map[String, _], String]#ReadOnlyContext, out: Collector[String]): Unit = {
        val cellKeyState: ReadOnlyBroadcastState[String, String] = ctx.getBroadcastState(cellKeyStateDescriptor)
        cellKeyState.immutableEntries().filter(_.getValue == value._2).foreach { rule =>
          out.collect(s"${value._1},${value._2},${rule.getKey}")
        }
      }

      override def processBroadcastElement(value: Map[String, _], ctx: BroadcastProcessFunction[(Long, String), Map[String, _], String]#Context, out: Collector[String]): Unit = {
        val cellKeyState = ctx.getBroadcastState(cellKeyStateDescriptor)

        value(Envelope.FieldName.OPERATION) match {
          case op if op == Envelope.Operation.READ.code() || op == Envelope.Operation.CREATE.code() =>
            val after = value(Envelope.FieldName.AFTER).asInstanceOf[Map[String, _]]
            val cronId = after("cronId").asInstanceOf[String]
            val cellKey = after("cellKey").asInstanceOf[String]
            cellKeyState.put(cronId, cellKey)
          case op if op == Envelope.Operation.DELETE.code =>
            val before = value(Envelope.FieldName.BEFORE).asInstanceOf[Map[String, _]]
            val cronId = before("cronId").asInstanceOf[String]
            cellKeyState.remove(cronId)
          case op if op == Envelope.Operation.UPDATE.code =>
            val before = value(Envelope.FieldName.BEFORE).asInstanceOf[Map[String, _]]
            val beforeCronId = before("cronId").asInstanceOf[String]
            cellKeyState.remove(beforeCronId)

            val after = value(Envelope.FieldName.AFTER).asInstanceOf[Map[String, _]]
            val cronId = after("cronId").asInstanceOf[String]
            val cellKey = after("cellKey").asInstanceOf[String]
            cellKeyState.put(cronId, cellKey)
          case _ =>
        }
      }
    }).print()

    env.execute()
  }
}
