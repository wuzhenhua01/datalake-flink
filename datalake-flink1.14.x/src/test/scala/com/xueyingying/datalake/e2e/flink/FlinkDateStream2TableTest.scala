package com.xueyingying.datalake.e2e.flink

import com.xueyingying.datalake.FlinkSuiteBase
import org.apache.flink.streaming.api.scala.createTypeInformation

class FlinkDateStream2TableTest extends FlinkSuiteBase {
  "to table" should "run" in {
    val tableResult = tabEnv.sqlQuery("SELECT id, content, op_ts, CAST(`date` AS STRING) AS `date` FROM datagen")
    val dataStream = tabEnv.toDataStream(tableResult).map(row => Payload(row.getField(0).asInstanceOf[Long], row.getField(1).asInstanceOf[String]))

    tabEnv.createTemporaryView("t1", dataStream)
    val t1Result = tabEnv.sqlQuery("SELECT id, DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyyMMddHH:mm:ss.SSS') FROM t1")
    tabEnv.toDataStream(t1Result).print()

    env.execute
  }
}

case class Payload(id: Long, content: String)
