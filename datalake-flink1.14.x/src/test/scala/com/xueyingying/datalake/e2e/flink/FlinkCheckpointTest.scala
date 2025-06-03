package com.xueyingying.datalake.e2e.flink

import java.util.concurrent.TimeUnit

import com.xueyingying.datalake.FlinkSuiteBase
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.types.Row

/**
 * @author cx330.1000ly@gmail.com
 * @version 1.0.0
 * @since 2024-12-02
 */
class FlinkCheckpointTest extends FlinkSuiteBase {
  "a" should "pass" in {
    val tableResult = tabEnv.sqlQuery("SELECT id, content, op_ts, CAST(`date` AS STRING) AS `date` FROM datagen")
    tabEnv.toDataStream(tableResult).map(new M()).print
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(10)
    env.execute()
  }
}

class M extends MapFunction[Row, String] with CheckpointedFunction {
  override def map(value: Row): String = {
    ""
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    println("============")
    TimeUnit.SECONDS.sleep(5)
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {

  }
}
