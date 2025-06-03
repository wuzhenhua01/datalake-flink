package com.xueyingying.datalake.e2e.flink

import com.xueyingying.datalake.FlinkSuiteBase
import org.apache.flink.streaming.api.scala.createTypeInformation

/**
 * @author cx330.1000ly@gmail.com
 * @version 1.0.0
 * @since 2024-12-04
 */
class FlinkDataStreamChainTest extends FlinkSuiteBase {
  "a" should "run" in {
    assertThrows[UnsupportedOperationException] {
      val tableResult = tabEnv.sqlQuery("SELECT id, content, op_ts, CAST(`date` AS STRING) AS `date` FROM datagen")
      tabEnv.toDataStream(tableResult)
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
}
