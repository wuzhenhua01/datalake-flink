package com.xueyingying.datalake.e2e.doris

import com.xueyingying.datalake.FlinkSuiteBase
import org.apache.doris.flink.cfg.{DorisExecutionOptions, DorisReadOptions}
import org.apache.doris.flink.sink.DorisSink
import org.apache.doris.flink.sink.writer.RowDataSerializer
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.table.data.{GenericRowData, RowData}

/**
 * @author wuzh8@asiainfo.com
 * @version 1.0.0
 * @since 2024-10-03
 */
class DorisFlinkDataStreamTest extends FlinkSuiteBase {
  "insert" should "run" in {
    import org.apache.doris.flink.cfg.DorisOptions
    val dorisOptions = DorisOptions.builder
      .setFenodes("10.19.87.18:8030")
      .setTableIdentifier("demo.student")
      .setUsername("root")
      .setPassword("")
      .build
    val executionOptions = DorisExecutionOptions.builder()
      .setBufferSize(3)
      .setCheckInterval(0)
      .setMaxRetries(3)
      .build()

    rowStream
      .map(_ => new GenericRowData(1).asInstanceOf[RowData])
      .sinkTo(DorisSink.builder()
        .setSerializer(RowDataSerializer.builder()
          .setFieldNames(Seq().toArray)
          .setFieldType(Seq().toArray)
          .build())
        .setDorisReadOptions(DorisReadOptions.defaults())
        .setDorisExecutionOptions(executionOptions)
        .setDorisOptions(dorisOptions)
        .build())
    env.execute()
  }

  "a" should "run" in {

  }
}
