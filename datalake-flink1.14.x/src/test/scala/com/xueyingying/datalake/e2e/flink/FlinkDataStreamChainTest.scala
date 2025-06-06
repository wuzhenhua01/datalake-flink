package com.xueyingying.datalake.e2e.flink

import com.xueyingying.datalake.FlinkSuiteBase
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

/**
 * @author cx330.1000ly@gmail.com
 * @version 1.0.0
 * @since 2024-12-04
 */
class FlinkDataStreamChainTest extends FlinkSuiteBase {
  "a" should "run" in {
    assertThrows[UnsupportedOperationException] {
      rowStream
        .map(row => Payload(row.getField(0).asInstanceOf[Long], row.getField(1).asInstanceOf[String]))
        .map(t => t)
        .map(t => t)
        .rebalance
        .startNewChain
        .map(t => t)
        .print()

      env.execute
    }
  }

  "chain" should "run" in {
    rowStream
      .map(v => v).name("1")
      .map(v => v).name("2")
      .map(v => v).name("3").startNewChain
      .map(v => v).name("4")
      .process(new ProcessFunction[Row, Row]() {
        override def processElement(value: Row, ctx: ProcessFunction[Row, Row]#Context, out: Collector[Row]): Unit = {
          out.collect(value)
        }
      }).name("5").startNewChain
      .map(v => v).name("6")
      .print

    env.execute
  }
}

case class Payload(id: Long, content: String)
