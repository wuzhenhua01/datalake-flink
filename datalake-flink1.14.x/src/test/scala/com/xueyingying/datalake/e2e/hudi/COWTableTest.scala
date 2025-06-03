package com.xueyingying.datalake.e2e.hudi

import java.time.{Instant, LocalDateTime}
import java.util.{Map => JMap}

import com.xueyingying.datalake.FlinkSuiteBase
import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory
import org.apache.flink.streaming.api.scala.{DataStream, createTypeInformation}
import org.apache.flink.table.data.{GenericRowData, RowData, StringData, TimestampData}
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.types.logical.{BigIntType, LogicalType, RowType, TimestampType, VarCharType}
import org.apache.flink.types.{Row, RowKind}
import org.apache.flink.util.Collector
import org.apache.hadoop.fs.Path
import org.apache.hudi.common.config.DFSPropertiesConfiguration
import org.apache.hudi.common.table.TableSchemaResolver
import org.apache.hudi.config.metrics.HoodieMetricsConfig
import org.apache.hudi.configuration.{FlinkOptions, OptionsInference}
import org.apache.hudi.keygen.constant.KeyGeneratorType
import org.apache.hudi.sink.utils.Pipelines
import org.apache.hudi.source.{StreamReadMonitoringFunction, StreamReadOperator}
import org.apache.hudi.table.format.mor.{MergeOnReadInputFormat, MergeOnReadInputSplit, MergeOnReadTableState}
import org.apache.hudi.table.format.{FilePathUtils, InternalSchemaManager}
import org.apache.hudi.util.{AvroSchemaConverter, StreamerUtil}

import scala.collection.JavaConversions.seqAsJavaList

/**
 * @author wuzh8@asiainfo.com
 * @version 1.0.0
 * @since 2024-10-09
 */
class COWTableTest extends FlinkSuiteBase {
  "COW" should "upsert" in {
    val tableResult = tabEnv.sqlQuery("SELECT id, content, op_ts, CAST(`date` AS STRING) AS `date` FROM datagen")
    val rowStream: DataStream[Row] = tabEnv.toDataStream(tableResult)
    val dataStream: DataStream[RowData] = rowStream.flatMap[RowData] { (data: Row, out: Collector[RowData]) =>
      val igenericRowData = new GenericRowData(4)
      igenericRowData.setRowKind(RowKind.INSERT)
      igenericRowData.setField(0, TimestampData.fromEpochMillis(Instant.now().getEpochSecond))
      igenericRowData.setField(1, data.getField("id"))
      igenericRowData.setField(2, StringData.fromString(data.getField("content").asInstanceOf[String]))
      igenericRowData.setField(3, StringData.fromString(data.getField("date").asInstanceOf[String]))
      out.collect(igenericRowData)
    }

    val hudiProps = DFSPropertiesConfiguration.getGlobalProps
    val hudiConf = Configuration.fromMap(hudiProps.asInstanceOf[JMap[String, String]])
    flinkConf.addAll(hudiConf)

    OptionsInference.setupSinkTasks(flinkConf, env.getParallelism)

    val rowType = RowType.of(false, Array(new TimestampType(3), new BigIntType, new VarCharType, new VarCharType), Array("_origin_op_ts", "id", "content", "date"))
    flinkConf.setString(FlinkOptions.SOURCE_AVRO_SCHEMA, AvroSchemaConverter.convertToSchema(rowType).toString)

    flinkConf.setString(FlinkOptions.PATH, "file:/tmp/t1")
    flinkConf.setString(FlinkOptions.TABLE_TYPE, FlinkOptions.TABLE_TYPE_COPY_ON_WRITE)
    flinkConf.setString(FlinkOptions.TABLE_NAME, "t1")
    flinkConf.setString(FlinkOptions.RECORD_KEY_FIELD, "id")
    flinkConf.setString(FlinkOptions.PRECOMBINE_FIELD, "_origin_op_ts")

    flinkConf.setString(FlinkOptions.KEYGEN_TYPE, KeyGeneratorType.NON_PARTITION.name)

    flinkConf.setBoolean(HoodieMetricsConfig.TURN_METRICS_ON.key, true)

    val hoodieRecordDataStream = Pipelines.bootstrap(flinkConf, rowType, dataStream.javaStream)
    val pipeline = Pipelines.hoodieStreamWrite(flinkConf, hoodieRecordDataStream)

    flinkConf.setInteger(FlinkOptions.CLEAN_RETAIN_COMMITS, 2)
    Pipelines.clean(flinkConf, pipeline)

    env.execute
  }

  "stream" should "read" in {
    val path = "file:/tmp/t1"
    OptionsInference.setupSourceTasks(flinkConf, env.getParallelism)
    flinkConf.setInteger(FlinkOptions.READ_STREAMING_CHECK_INTERVAL, 10)
    val metaClient = StreamerUtil.createMetaClient(path, conf)
    val schemaResolver = new TableSchemaResolver(metaClient)
    val tableAvroSchema = schemaResolver.getTableAvroSchema
    val tableRowDataType = AvroSchemaConverter.convertToDataType(tableAvroSchema)
    val tableRowType = tableRowDataType.getLogicalType.asInstanceOf[RowType]
    val monitoringFunction = new StreamReadMonitoringFunction(flinkConf, FilePathUtils.toFlinkPath(new Path(path)), tableRowType, 1 << 30, Option.empty.orNull)
    val parallelism = flinkConf.getInteger(FlinkOptions.READ_TASKS)
    val dataStream = env.addSource(monitoringFunction)
      .uid(Pipelines.opUID("split_monitor", flinkConf))
      .setParallelism(1)
      .setMaxParallelism(1)
      .partitionCustom(new Partitioner[Int]() {
        override def partition(splitNum: Int, maxParallelism: Int): Int = splitNum % parallelism
      }, mergeOnReadInputSplit => mergeOnReadInputSplit.getSplitNumber)

    val internalSchemaManager = InternalSchemaManager.get(flinkConf, metaClient)
    val requiredRowType = RowType.of(false, Array[LogicalType](new VarCharType), Array("content"))
    val hoodieTableState = new MergeOnReadTableState(
      tableRowType,
      requiredRowType,
      tableAvroSchema.toString,
      AvroSchemaConverter.convertToSchema(requiredRowType).toString,
      Nil,
      Nil.toArray)
    val inputFormat = MergeOnReadInputFormat.builder
      .config(flinkConf)
      .tableState(hoodieTableState)
      .fieldTypes(tableRowDataType.getChildren)
      .predicates(Nil)
      .limit(-1)
      .emitDelete(false)
      .internalSchemaManager(internalSchemaManager)
      .build

    val factory: OneInputStreamOperatorFactory[MergeOnReadInputSplit, RowData] = StreamReadOperator.factory(inputFormat)
    dataStream
      .javaStream
      .transform("split_reader", InternalTypeInfo.of(requiredRowType), factory)
      .print()

    env.execute
  }
}
