package com.xueyingying.datalake.e2e.hudi

import java.time.LocalDateTime
import java.util.concurrent.TimeUnit
import java.util.{Map => JMap}

import com.xueyingying.datalake.FlinkSuiteBase
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, createTypeInformation}
import org.apache.flink.table.data.{GenericRowData, RowData, StringData, TimestampData}
import org.apache.flink.table.types.logical.{BigIntType, LogicalType, RowType, TimestampType, VarCharType}
import org.apache.flink.types.{Row, RowKind}
import org.apache.flink.util.Collector
import org.apache.hudi.common.config.DFSPropertiesConfiguration
import org.apache.hudi.configuration.{FlinkOptions, OptionsInference}
import org.apache.hudi.keygen.constant.KeyGeneratorType
import org.apache.hudi.sink.utils.Pipelines
import org.apache.hudi.util.AvroSchemaConverter

/**
 * @author wuzh8@asiainfo.com
 * @version 1.0.0
 * @since 2024-12-08
 */
class MORTableTest extends FlinkSuiteBase {
  "MOR" should "upsert" in {
    val dataStream: DataStream[RowData] = rowStream.flatMap[RowData] { (data: Row, out: Collector[RowData]) =>
      val igenericRowData = new GenericRowData(4)
      igenericRowData.setRowKind(RowKind.INSERT)
      igenericRowData.setField(0, TimestampData.fromLocalDateTime(data.getField("op_ts").asInstanceOf[LocalDateTime]))
      igenericRowData.setField(1, data.getField("id"))
      igenericRowData.setField(2, StringData.fromString(data.getField("content").asInstanceOf[String]))
      igenericRowData.setField(3, StringData.fromString(data.getField("date").asInstanceOf[String]))
      out.collect(igenericRowData)
    }

    val hudiProps = DFSPropertiesConfiguration.getGlobalProps
    val hudiConf = Configuration.fromMap(hudiProps.asInstanceOf[JMap[String, String]])
    flinkConf.addAll(hudiConf)

    OptionsInference.setupSinkTasks(flinkConf, env.getParallelism)

    val rowType = RowType.of(false, Array(new TimestampType, new BigIntType, new VarCharType, new VarCharType), Array("_origin_op_ts", "id", "content", "date"))
    flinkConf.setString(FlinkOptions.SOURCE_AVRO_SCHEMA, AvroSchemaConverter.convertToSchema(rowType).toString)

    flinkConf.setString(FlinkOptions.PATH, "file:/tmp/t1")
    flinkConf.setString(FlinkOptions.TABLE_TYPE, FlinkOptions.TABLE_TYPE_MERGE_ON_READ)
    flinkConf.setString(FlinkOptions.TABLE_NAME, "t1")
    flinkConf.setString(FlinkOptions.RECORD_KEY_FIELD, "id")
    flinkConf.setString(FlinkOptions.PRECOMBINE_FIELD, "_origin_op_ts")

    flinkConf.setString(FlinkOptions.KEYGEN_TYPE, KeyGeneratorType.NON_PARTITION.name)

    flinkConf.setString(FlinkOptions.COMPACTION_TRIGGER_STRATEGY, FlinkOptions.TIME_ELAPSED)
    flinkConf.setInteger(FlinkOptions.COMPACTION_DELTA_SECONDS, TimeUnit.SECONDS.toSeconds(120).toInt)

    val hoodieRecordDataStream = Pipelines.bootstrap(flinkConf, rowType, dataStream.javaStream)
    val pipeline = Pipelines.hoodieStreamWrite(flinkConf, hoodieRecordDataStream)
    Pipelines.compact(flinkConf, pipeline)

    env.execute
  }

  "MOR" should "run" in {
    val dataStream: DataStream[RowData] = env.fromElements("", "", "").map { _ =>
      val igenericRowData = new GenericRowData(4)
      igenericRowData.setRowKind(RowKind.INSERT)
      igenericRowData.setField(0, StringData.fromString("1"))
      igenericRowData.setField(1, StringData.fromString("1"))
      igenericRowData.setField(2, StringData.fromString("1"))
      igenericRowData.setField(3, StringData.fromString("1"))
      igenericRowData
    }

    val hudiProps = DFSPropertiesConfiguration.getGlobalProps
    val hudiConf = Configuration.fromMap(hudiProps.asInstanceOf[JMap[String, String]])
    flinkConf.addAll(hudiConf)

    OptionsInference.setupSinkTasks(flinkConf, env.getParallelism)

    val rowType = RowType.of(false, Array[LogicalType](new VarCharType, new VarCharType, new VarCharType, new VarCharType), Array("_origin_op_ts", "id", "content", "date"))
    flinkConf.setString(FlinkOptions.SOURCE_AVRO_SCHEMA, AvroSchemaConverter.convertToSchema(rowType).toString)

    flinkConf.setString(FlinkOptions.PATH, "file:/tmp/t1")
    flinkConf.setString(FlinkOptions.TABLE_TYPE, FlinkOptions.TABLE_TYPE_MERGE_ON_READ)
    flinkConf.setString(FlinkOptions.TABLE_NAME, "t1")
    flinkConf.setString(FlinkOptions.RECORD_KEY_FIELD, "id")
    flinkConf.setString(FlinkOptions.PRECOMBINE_FIELD, "_origin_op_ts")

    flinkConf.setString(FlinkOptions.KEYGEN_TYPE, KeyGeneratorType.NON_PARTITION.name)

    flinkConf.setString(FlinkOptions.COMPACTION_TRIGGER_STRATEGY, FlinkOptions.TIME_ELAPSED)
    flinkConf.setInteger(FlinkOptions.COMPACTION_DELTA_SECONDS, TimeUnit.SECONDS.toSeconds(120).toInt)

    val hoodieRecordDataStream = Pipelines.bootstrap(flinkConf, rowType, dataStream.javaStream)
    val pipeline = Pipelines.hoodieStreamWrite(flinkConf, hoodieRecordDataStream)
    Pipelines.compact(flinkConf, pipeline)

    env.execute
  }
}
