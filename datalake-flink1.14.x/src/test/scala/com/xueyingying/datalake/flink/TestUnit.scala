package com.xueyingying.datalake.flink

import org.apache.flink.util.TimeUtils
import org.scalatest.flatspec.AnyFlatSpec

class TestUnit  extends AnyFlatSpec{
  "parse" should "success" in {
    println(TimeUtils.parseDuration("2 min").toMinutes)
    println(TimeUtils.parseDuration("2 minute").toMinutes)
  }
}
