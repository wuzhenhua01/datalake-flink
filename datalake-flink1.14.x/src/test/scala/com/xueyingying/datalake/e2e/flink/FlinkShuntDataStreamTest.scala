package com.xueyingying.datalake.e2e.flink

import java.util.UUID

import com.xueyingying.datalake.FlinkSuiteBase
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, createTypeInformation}
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

import scala.collection.breakOut

/**
 * @author cx330.1000ly@gmail.com
 * @version 1.0.0
 * @since 2023-01-10
 */
class FlinkShuntDataStreamTest extends FlinkSuiteBase {
  "shunt" should "work" in {
    val tagMap: Map[Int, OutputTag[String]] = (for (i <- 0 to 5) yield i -> OutputTag[String](i.toString)) (breakOut)
    val tagStream = rowStream.process(new ProcessFunction[Row, String] {
      override def processElement(data: Row, ctx: ProcessFunction[Row, String]#Context, out: Collector[String]): Unit = {
        val id = data.getField("id").asInstanceOf[Long]
        val content = data.getField("content").asInstanceOf[String]

        val idx = id % 5
        val tag = tagMap(idx.toInt)

        ctx.output(tag, content)
      }
    })

    var slotSharingGroup = UUID.randomUUID.toString
    tagMap.zipWithIndex.map(kv => (kv._1._1, kv._1._2, kv._2)).foreach {
      case (idx, tag, id) =>
        if (id % 5 == 0) {
          slotSharingGroup = UUID.randomUUID.toString
        }
        println(slotSharingGroup)
        val shuntStream = tagStream.getSideOutput(tag)
        shuntStream
          .filter(_ => false).slotSharingGroup(slotSharingGroup)
          .print(idx.toString)
    }

    env.execute
  }
}
