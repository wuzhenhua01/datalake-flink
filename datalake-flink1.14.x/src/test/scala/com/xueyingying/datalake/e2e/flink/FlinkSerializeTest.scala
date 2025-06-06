package com.xueyingying.datalake.e2e.flink

import java.time.format.DateTimeFormatter

import com.xueyingying.datalake.FlinkSuiteBase
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.types.Row

/**
 * @author cx330.1000ly@gmail.com
 * @version 1.0.0
 * @since 2025-06-04
 */
class FlinkSerializeTest extends FlinkSuiteBase {
  "a" should "run" in {
    rowStream
      .map(new RichMapFunction[Row, Row] {
        var dayIdFormatter: DateTimeFormatter = _

        override def open(parameters: Configuration): Unit = {
          dayIdFormatter = DateTimeFormatter.BASIC_ISO_DATE
        }

        override def map(value: Row): Row = {
          println(dayIdFormatter)
          value
        }
      })
      .print
    env.execute
  }
}
