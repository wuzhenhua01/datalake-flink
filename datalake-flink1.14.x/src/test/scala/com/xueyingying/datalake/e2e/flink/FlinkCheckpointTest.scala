package com.xueyingying.datalake.e2e.flink

import java.util.concurrent.TimeUnit

import com.xueyingying.datalake.FlinkSuiteBase
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

/**
 * @author cx330.1000ly@gmail.com
 * @version 1.0.0
 * @since 2024-12-02
 */
class FlinkCheckpointTest extends FlinkSuiteBase {
  "a" should "pass" in {
    rowStream.map(new M())
      .process(new ProcessFunction[String, String]() {
        override def processElement(value: String, ctx: ProcessFunction[String, String]#Context, out: Collector[String]): Unit = {
          if (ctx.timerService().currentProcessingTime() / 1000 == 0) {
            println(ctx.timerService().currentProcessingTime())
          }
        }
      })
      .print
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(10)
    env.execute()
  }
}

class M extends MapFunction[Row, String] with CheckpointedFunction {
  override def map(value: Row): String = {
    "aaa"
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    println("============")
    TimeUnit.SECONDS.sleep(5)
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {

  }
}
