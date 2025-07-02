package com.xueyingying.datalake.e2e.flink

import com.xueyingying.datalake.FlinkSuiteBase
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.types.Row

import scala.collection.mutable.ListBuffer

/**
 * @author cx330.1000ly@gmail.com
 * @version 1.0.0
 * @since 2024-12-02
 */
class FlinkStateTest extends FlinkSuiteBase {
  "a" should "run" in {
    rowStream.keyBy(_.getField(0).asInstanceOf[Int]).map(new S()).print
    env.execute()
  }
}

class S extends RichMapFunction[Row, String] {
  var state: ValueState[ListBuffer[String]] = _

  override def open(parameters: Configuration): Unit = {
    val stateDescriptor = new ValueStateDescriptor("lbs", classOf[ListBuffer[String]])
    state = getRuntimeContext.getState(stateDescriptor)
  }

  override def map(value: Row): String = {
    var listBuffer = state.value()
    println(listBuffer)
    if (listBuffer == null) {
      listBuffer = ListBuffer()
      state.update(listBuffer)
    }
    listBuffer += "a"
    ""
  }
}

