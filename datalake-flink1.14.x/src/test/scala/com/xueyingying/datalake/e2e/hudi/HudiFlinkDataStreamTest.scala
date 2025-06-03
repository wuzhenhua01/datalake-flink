package com.xueyingying.datalake.e2e.hudi

import java.time.{Instant, LocalDateTime}
import java.util.concurrent.TimeUnit
import java.util.{Map => JMap}

import com.xueyingying.datalake.FlinkSuiteBase
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunction
import org.apache.flink.streaming.api.operators.KeyedProcessOperator
import org.apache.flink.streaming.api.scala.{DataStream, createTypeInformation}
import org.apache.flink.table.data.{GenericRowData, RowData, StringData, TimestampData}
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.{BigIntType, RowType, TimestampType, VarCharType}
import org.apache.flink.types.{Row, RowKind}
import org.apache.flink.util.Collector
import org.apache.hudi.common.config.DFSPropertiesConfiguration
import org.apache.hudi.common.model.{HoodieRecord, WriteOperationType}
import org.apache.hudi.common.table.TableSchemaResolver
import org.apache.hudi.config.metrics.HoodieMetricsConfig
import org.apache.hudi.config.{HoodieArchivalConfig, HoodieCompactionConfig}
import org.apache.hudi.configuration.{FlinkOptions, OptionsInference}
import org.apache.hudi.keygen.constant.KeyGeneratorType
import org.apache.hudi.sink.StreamWriteOperator
import org.apache.hudi.sink.bootstrap.BootstrapOperator
import org.apache.hudi.sink.compact.{CompactOperator, CompactionCommitEvent, CompactionCommitSink, CompactionPlanEvent, CompactionPlanOperator}
import org.apache.hudi.sink.partitioner.BucketAssignFunction
import org.apache.hudi.sink.transform.RowDataToHoodieFunctions
import org.apache.hudi.sink.utils.Pipelines
import org.apache.hudi.util.{AvroSchemaConverter, StreamerUtil}

/**
 * @author cx330.1000ly@gmail.com
 * @version 1.0.0
 * @since 2023-01-10
 */
class HudiFlinkDataStreamTest extends FlinkSuiteBase {
  "hudi" should "append" in {
    val tableResult = tabEnv.sqlQuery("SELECT id, content, op_ts, CAST(`date` AS STRING) AS `date` FROM datagen")
    val rowStream: DataStream[Row] = tabEnv.toDataStream(tableResult)
    val dataStream: DataStream[RowData] = rowStream.flatMap[RowData] { (data: Row, out: Collector[RowData]) =>
      val igenericRowData = new GenericRowData(4)
      igenericRowData.setRowKind(RowKind.INSERT)
      igenericRowData.setField(0, TimestampData.fromEpochMillis(Instant.now().toEpochMilli))
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
    flinkConf.set(FlinkOptions.SOURCE_AVRO_SCHEMA, AvroSchemaConverter.convertToSchema(rowType).toString)

    flinkConf.set(FlinkOptions.PATH, "file:/tmp/t1")
    flinkConf.set(FlinkOptions.TABLE_TYPE, FlinkOptions.TABLE_TYPE_COPY_ON_WRITE) // cow,mor append模式下没有区别
    flinkConf.set(FlinkOptions.TABLE_NAME, "t1")

    flinkConf.set(FlinkOptions.KEYGEN_TYPE, KeyGeneratorType.COMPLEX.name)
    flinkConf.set(FlinkOptions.PARTITION_PATH_FIELD, "id,date")
    flinkConf.set(FlinkOptions.HIVE_STYLE_PARTITIONING, Boolean.box(true))

    flinkConf.setBoolean(HoodieMetricsConfig.TURN_METRICS_ON.key, Boolean.box(true))

    flinkConf.set(FlinkOptions.OPERATION, WriteOperationType.INSERT.value)
    val pipeline = Pipelines.append(flinkConf, rowType, dataStream.javaStream)

    // StreamWriteOperatorCoordinator#scheduleTableServices
    // 生成replacecommit
    flinkConf.set(FlinkOptions.CLUSTERING_SCHEDULE_ENABLED, Boolean.box(true))

    flinkConf.set(FlinkOptions.CLUSTERING_ASYNC_ENABLED, Boolean.box(true))
    flinkConf.set(FlinkOptions.CLUSTERING_PLAN_STRATEGY_PARTITION_REGEX_PATTERN, "")

    flinkConf.set(FlinkOptions.CLEAN_RETAIN_COMMITS, Int.box(2))
    Pipelines.cluster(flinkConf, rowType, pipeline)

    env.execute
  }

  "hudi data stream write" should "work" in {
    val tableResult = tabEnv.sqlQuery("SELECT id, content, op_ts, CAST(`date` AS STRING) AS `date` FROM datagen")
    val rowStream: DataStream[Row] = tabEnv.toDataStream(tableResult)
    val rowDataStream: DataStream[RowData] = rowStream.flatMap[RowData] { (data: Row, out: Collector[RowData]) =>
      val igenericRowData = new GenericRowData(4)
      igenericRowData.setRowKind(RowKind.INSERT)
      igenericRowData.setField(0, TimestampData.fromLocalDateTime(data.getField("op_ts").asInstanceOf[LocalDateTime]).getMillisecond)
      igenericRowData.setField(1, data.getField("id"))
      igenericRowData.setField(2, StringData.fromString(data.getField("content").asInstanceOf[String]))
      igenericRowData.setField(3, StringData.fromString(data.getField("date").asInstanceOf[String]))
      out.collect(igenericRowData)
    }.slotSharingGroup("aa")

    val hudiProps = DFSPropertiesConfiguration.getGlobalProps
    val hudiConf = Configuration.fromMap(hudiProps.asInstanceOf[JMap[String, String]])
    flinkConf.addAll(hudiConf)

    OptionsInference.setupSinkTasks(flinkConf, env.getParallelism)

    val rowType = RowType.of(false, Array(new BigIntType, new BigIntType, new VarCharType, new VarCharType), Array("_origin_op_ts", "id", "content", "date"))
    flinkConf.set(FlinkOptions.SOURCE_AVRO_SCHEMA, AvroSchemaConverter.convertToSchema(rowType).toString)

    flinkConf.set(FlinkOptions.PATH, "file:/tmp/t2")
    flinkConf.set(FlinkOptions.TABLE_TYPE, FlinkOptions.TABLE_TYPE_MERGE_ON_READ)
    flinkConf.set(FlinkOptions.TABLE_NAME, "t2")
    flinkConf.set(FlinkOptions.PRECOMBINE_FIELD, "_origin_op_ts")
    flinkConf.set(FlinkOptions.RECORD_KEY_FIELD, "id")

    flinkConf.set(FlinkOptions.HIVE_SYNC_ENABLED, Boolean.box(false))
    flinkConf.set(FlinkOptions.HIVE_SYNC_DB, "default")
    flinkConf.set(FlinkOptions.HIVE_SYNC_TABLE, "t2")
    flinkConf.set(FlinkOptions.HIVE_SYNC_CONF_DIR, "/cluster/flinkConf")

    flinkConf.set(FlinkOptions.HIVE_STYLE_PARTITIONING, Boolean.box(true))
    flinkConf.set(FlinkOptions.KEYGEN_TYPE, KeyGeneratorType.NON_PARTITION.name)

    flinkConf.setString(HoodieCompactionConfig.PARQUET_SMALL_FILE_LIMIT.key, 0.toString)
    flinkConf.set(FlinkOptions.COMPACTION_TRIGGER_STRATEGY, FlinkOptions.TIME_ELAPSED)
    flinkConf.set(FlinkOptions.COMPACTION_DELTA_SECONDS, Int.box(TimeUnit.SECONDS.toSeconds(5).toInt))

    flinkConf.setString(HoodieArchivalConfig.COMMITS_ARCHIVAL_BATCH_SIZE.key, 1.toString)

    val operatorFactory = StreamWriteOperator.getFactory[HoodieRecord[_]](flinkConf)
    val compactionPlan2FileId = new KeySelector[CompactionPlanEvent, String]() {
      override def getKey(plan: CompactionPlanEvent): String = plan.getOperation.getFileGroupId.getFileId
    }
    rowDataStream
      .map(RowDataToHoodieFunctions.create(rowType, flinkConf))
      .name(Pipelines.opName("row_data_to_hoodie_record", flinkConf))
      .transform(Pipelines.opName("index_bootstrap", flinkConf), new BootstrapOperator[HoodieRecord[_], HoodieRecord[_]](flinkConf))
      .uid(Pipelines.opUID("index_bootstrap", flinkConf))
      .keyBy(_.getRecordKey)
      .transform(Pipelines.opName("bucket_assigner", flinkConf), new KeyedProcessOperator[String, HoodieRecord[_], HoodieRecord[_]](new BucketAssignFunction[String, HoodieRecord[_], HoodieRecord[_]](flinkConf)))
      .uid(Pipelines.opUID("bucket_assigner", flinkConf))
      .setParallelism(flinkConf.getInteger(FlinkOptions.BUCKET_ASSIGN_TASKS))
      .keyBy(_.getCurrentLocation.getFileId)
      .javaStream
      .transform(Pipelines.opName("stream_write", flinkConf), TypeInformation.of(classOf[AnyRef]), operatorFactory)
      .uid(Pipelines.opUID("stream_write", flinkConf))
      .setParallelism(flinkConf.getInteger(FlinkOptions.WRITE_TASKS))
      .transform(Pipelines.opName("compact_plan_generate", flinkConf), TypeInformation.of(classOf[CompactionPlanEvent]), new CompactionPlanOperator(flinkConf))
      .slotSharingGroup("compact")
      .setParallelism(1) // plan generate must be singleton
      .keyBy(compactionPlan2FileId)
      .transform(Pipelines.opName("compact_task", flinkConf), TypeInformation.of(classOf[CompactionCommitEvent]), new CompactOperator(flinkConf))
      .setParallelism(flinkConf.getInteger(FlinkOptions.COMPACTION_TASKS))
      .slotSharingGroup("compact")
      .addSink(new CompactionCommitSink(flinkConf))
      .name(Pipelines.opName("compact_commit", flinkConf))
      .setParallelism(1) // compaction commit should be singleton
      .slotSharingGroup("compact")

    env.execute
  }

  "hudi" should "clean" in {
    val tableResult = tabEnv.sqlQuery("SELECT id, content, op_ts, CAST(`date` AS STRING) AS `date` FROM datagen")
    val rowStream: DataStream[Row] = tabEnv.toDataStream(tableResult)
    val dataStream: DataStream[RowData] = rowStream.flatMap[RowData] { (data: Row, out: Collector[RowData]) =>
      val igenericRowData = new GenericRowData(4)
      igenericRowData.setRowKind(RowKind.INSERT)
      igenericRowData.setField(0, TimestampData.fromEpochMillis(Instant.now().toEpochMilli))
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
    flinkConf.set(FlinkOptions.SOURCE_AVRO_SCHEMA, AvroSchemaConverter.convertToSchema(rowType).toString)

    flinkConf.set(FlinkOptions.PATH, "file:/tmp/t1")
    flinkConf.set(FlinkOptions.TABLE_TYPE, FlinkOptions.TABLE_TYPE_COPY_ON_WRITE) // cow,mor append模式下没有区别
    flinkConf.set(FlinkOptions.TABLE_NAME, "t1")

/*
    flinkConf.setString(FlinkOptions.KEYGEN_TYPE, KeyGeneratorType.SIMPLE.name)
    flinkConf.setString(FlinkOptions.PARTITION_PATH_FIELD, "date")
    flinkConf.setBoolean(FlinkOptions.HIVE_STYLE_PARTITIONING, true)
*/

    flinkConf.setBoolean(HoodieMetricsConfig.TURN_METRICS_ON.key, true)

    flinkConf.set(FlinkOptions.OPERATION, WriteOperationType.INSERT.value)
    val pipeline = Pipelines.append(flinkConf, rowType, dataStream.javaStream)

    flinkConf.set(FlinkOptions.CLEAN_RETAIN_COMMITS, Int.box(2))
    Pipelines.cluster(flinkConf, rowType, pipeline)

    env.execute
  }

  "t1" should "read" in {
    tabEnv.executeSql(
      s"""
         |CREATE TABLE t1 (
         |  _origin_op_ts TIMESTAMP,
         |  id BIGINT,
         |  content STRING,
         |  `date` STRING
         |)
         |WITH (
         |  'connector'='hudi',
         |  '${FlinkOptions.PATH.key}'='file:/tmp/t1'
         |)
      """.stripMargin)
    tabEnv.executeSql(
      """
        |SELECT COUNT(1)
        |FROM t1
      """.stripMargin).print()
  }

  "hudi" should "read" in {
    val path = "file:/tmp/t1"
    val metaClient = StreamerUtil.createMetaClient(path, conf)
    val schemaResolver = new TableSchemaResolver(metaClient)
    val tableAvroSchema = schemaResolver.getTableAvroSchema
    val rowDataType = AvroSchemaConverter.convertToDataType(tableAvroSchema)
    val rowType = rowDataType.getLogicalType.asInstanceOf[RowType]

  }
}
