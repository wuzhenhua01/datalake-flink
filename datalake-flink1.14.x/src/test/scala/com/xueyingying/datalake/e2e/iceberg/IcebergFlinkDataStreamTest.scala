package com.xueyingying.datalake.e2e.iceberg

import java.time.LocalDateTime

import com.xueyingying.datalake.FlinkSuiteBase
import org.apache.flink.streaming.api.scala.{DataStream, createTypeInformation}
import org.apache.flink.table.data.{GenericRowData, RowData, StringData, TimestampData}
import org.apache.flink.types.{Row, RowKind}
import org.apache.flink.util.Collector
import org.apache.iceberg.flink.TableLoader
import org.apache.iceberg.flink.sink.FlinkSink

import scala.collection.JavaConversions.seqAsJavaList

/**
 * @author cx330.1000ly@gmail.com
 * @version 1.0.0
 * @since 2023-09-14
 */
class IcebergFlinkDataStreamTest extends FlinkSuiteBase {
  "iceberg" should "write" in {
    val rowDataDataStream: DataStream[RowData] = rowStream.flatMap[RowData] { (data: Row, out: Collector[RowData]) =>
      val igenericRowData = new GenericRowData(4)
      igenericRowData.setRowKind(RowKind.INSERT)
      igenericRowData.setField(0, TimestampData.fromLocalDateTime(data.getField("op_ts").asInstanceOf[LocalDateTime]))
      igenericRowData.setField(1, data.getField("id"))
      igenericRowData.setField(2, StringData.fromString(data.getField("content").asInstanceOf[String]))
      igenericRowData.setField(3, StringData.fromString(data.getField("date").asInstanceOf[String]))
      out.collect(igenericRowData)
    }.slotSharingGroup("aa")

    val t1 = TableLoader.fromHadoopTable("file:/tmp/t1")
    FlinkSink.forRowData(rowDataDataStream.javaStream)
      .tableLoader(t1)
      .equalityFieldColumns(List("a", "b"))
      .append

    env.execute
  }
}
