package com.xueyingying.datalake.e2e.lbs

import java.lang.{Long => JLong}
import java.util.concurrent.TimeUnit
import java.util.{Map => JMap}

import com.google.common.collect.{Range, TreeRangeMap}
import com.xueyingying.datalake.FlinkSuiteBase
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.table.data.{GenericRowData, RowData, StringData}
import org.apache.flink.table.types.logical.{BigIntType, RowType, VarCharType}
import org.apache.flink.types.RowKind
import org.apache.flink.util.Collector
import org.apache.hudi.common.config.DFSPropertiesConfiguration
import org.apache.hudi.configuration.{FlinkOptions, OptionsInference}
import org.apache.hudi.keygen.constant.KeyGeneratorType
import org.apache.hudi.sink.utils.Pipelines
import org.apache.hudi.util.AvroSchemaConverter

/**
 * @author cx330.1000ly@gmail.com
 * @version 1.0.0
 * @since 2023-09-14
 */
class KafkaFlinkDataStreamTest extends FlinkSuiteBase {
  "kafka" should "write" in {
    val dataStream = env.fromSource(KafkaSource.builder[String]()
      .setBootstrapServers("xueyingying.com:9094")
      .setTopics("lbs")
      .setGroupId("lbs")
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build(), WatermarkStrategy.noWatermarks[String], "kafka source")
      .map(_.split(","))
      .map(row => (row(0), row(1), row(2).toLong))
      .keyBy(_._1)
      .process(new KeyedProcessFunction[String, (String, String, Long), RowData]() {
        var state: MapState[String, TreeRangeMap[JLong, (String, Set[Long])]] = _

        override def open(parameters: Configuration): Unit = {
          val stateDescriptor = new MapStateDescriptor[String, TreeRangeMap[JLong, (String, Set[Long])]]("lbs", TypeInformation.of(classOf[String]), TypeInformation.of(classOf[TreeRangeMap[JLong, (String, Set[Long])]]))
          state = getRuntimeContext.getMapState(stateDescriptor)
        }

        override def processElement(value: (String, String, Long), ctx: KeyedProcessFunction[String, (String, String, Long), RowData]#Context, out: Collector[RowData]): Unit = {
          if (!state.contains(ctx.getCurrentKey)) {
            val range = TreeRangeMap.create[JLong, (String, Set[Long])]
            range.put(Range.atLeast(value._3), (value._2, Set(value._3)))
            state.put(ctx.getCurrentKey, range)
            val igenericRowData = new GenericRowData(4)
            igenericRowData.setRowKind(RowKind.INSERT)
            igenericRowData.setField(0, StringData.fromString(value._1))
            igenericRowData.setField(1, StringData.fromString(value._2))
            igenericRowData.setField(2, value._3)
            igenericRowData.setField(3, value._3)
            out.collect(igenericRowData)
            return
          }

          val range = state.get(ctx.getCurrentKey)
          Option(range.getEntry(value._3)) match {
            case None =>
              newSegment(value, out, range)
            case Some(timeSegment: JMap.Entry[Range[JLong], (String, Set[Long])]) =>
              val timeSegmentKey = timeSegment.getKey
              val lbsState = timeSegment.getValue
              // 1.同一个位置,顺序数据,更新时间戳
              if (lbsState._1 == value._2) {
                updateSegment(value, out, range, timeSegment)
                return
              }

              // 2. 发生位置更新
              // 2.1 最新时间片
              if (!timeSegmentKey.hasUpperBound) {
                val maxTs = lbsState._2.max
                // 2.1.1. 顺序数据,追加轨迹片,自动切片
                if (value._3 >= maxTs) {
                  newSegment(value, out, range)
                  return
                }
                // 2.1.2. 延迟数据,切分轨迹片
                lbsState._2.groupBy(_ > value._3).foreach {
                  case (true, next) =>
                    range.put(Range.closedOpen(value._3, next.min), (value._2, Set(value._3)))
                    val igenericRowData = new GenericRowData(4)
                    igenericRowData.setRowKind(RowKind.INSERT)
                    igenericRowData.setField(0, value._1)
                    igenericRowData.setField(1, value._2)
                    igenericRowData.setField(2, value._3)
                    igenericRowData.setField(3, value._3)
                    out.collect(igenericRowData)

                    range.put(Range.atLeast(next.min), (value._2, next))
                    val rowData2 = new GenericRowData(4)
                    rowData2.setRowKind(RowKind.INSERT)
                    rowData2.setField(0, value._1)
                    rowData2.setField(1, value._2)
                    rowData2.setField(2, next.min)
                    rowData2.setField(3, next.max)
                    out.collect(rowData2)
                  case (false, previous: Set[Long]) =>
                    range.put(Range.closedOpen(previous.min, value._3), (value._2, previous))
                    val ubgenericRowData = new GenericRowData(4)
                    ubgenericRowData.setRowKind(RowKind.UPDATE_BEFORE)
                    ubgenericRowData.setField(0, lbsState._1)
                    ubgenericRowData.setField(1, value._2)
                    ubgenericRowData.setField(2, timeSegmentKey.lowerEndpoint)
                    ubgenericRowData.setField(3, lbsState._2.max)
                    out.collect(ubgenericRowData)

                    val uagenericRowData = new GenericRowData(4)
                    uagenericRowData.setRowKind(RowKind.UPDATE_AFTER)
                    uagenericRowData.setField(0, value._1)
                    uagenericRowData.setField(1, value._2)
                    uagenericRowData.setField(2, timeSegmentKey.lowerEndpoint)
                    uagenericRowData.setField(3, previous.max)
                    out.collect(uagenericRowData)
                }
                return
              }

              // 2.2. 历史时间片
              val serialTsSplits = lbsState._2.groupBy(_ > value._3)
              val previousSerialTsSplit = serialTsSplits.get(false)
              val nextSerialTsSplit = serialTsSplits.get(true)
              // 2.2.1. 插入时间片中间
              if (nextSerialTsSplit.isDefined) {
                range.put(Range.closedOpen(timeSegmentKey.lowerEndpoint(), value._3), (lbsState._1, previousSerialTsSplit.get))
                val ubgenericRowData = new GenericRowData(4)
                ubgenericRowData.setRowKind(RowKind.UPDATE_BEFORE)
                ubgenericRowData.setField(0, value._1)
                ubgenericRowData.setField(1, lbsState._1)
                ubgenericRowData.setField(2, timeSegmentKey.lowerEndpoint)
                ubgenericRowData.setField(3, lbsState._2.max)
                out.collect(ubgenericRowData)

                val uagenericRowData = new GenericRowData(4)
                uagenericRowData.setRowKind(RowKind.UPDATE_AFTER)
                uagenericRowData.setField(0, value._1)
                uagenericRowData.setField(1, lbsState._1)
                uagenericRowData.setField(2, timeSegmentKey.lowerEndpoint)
                uagenericRowData.setField(3, previousSerialTsSplit.get.max)
                out.collect(uagenericRowData)

                range.put(Range.closedOpen(value._3, nextSerialTsSplit.get.min), (value._2, Set(value._3)))
                val igenericRowData = new GenericRowData(4)
                igenericRowData.setRowKind(RowKind.INSERT)
                igenericRowData.setField(0, value._1)
                igenericRowData.setField(1, value._2)
                igenericRowData.setField(2, value._3)
                igenericRowData.setField(3, value._3)
                out.collect(igenericRowData)

                range.put(Range.closedOpen(nextSerialTsSplit.get.min, timeSegmentKey.upperEndpoint()), (lbsState._1, nextSerialTsSplit.get))
                val rowData2 = new GenericRowData(4)
                rowData2.setRowKind(RowKind.INSERT)
                rowData2.setField(0, value._1)
                rowData2.setField(1, value._2)
                rowData2.setField(2, nextSerialTsSplit.get.min)
                rowData2.setField(3, nextSerialTsSplit.get.max)
                out.collect(rowData2)
                return
              }

              // 2.2.2 插入时间片结尾
              val nextTimeSegment = range.getEntry(timeSegmentKey.upperEndpoint())
              val nextTimeSegmentKey = nextTimeSegment.getKey
              val nextLbsState = nextTimeSegment.getValue

              value._2 match {
                // 与下一个时间片合并
                case nextLbsState._1 =>
                  val dgenericRowData = new GenericRowData(4)
                  dgenericRowData.setRowKind(RowKind.DELETE)
                  dgenericRowData.setField(0, value._1)
                  dgenericRowData.setField(1, lbsState._1)
                  dgenericRowData.setField(2, nextTimeSegmentKey.lowerEndpoint)
                  dgenericRowData.setField(3, nextLbsState._2.max)
                  out.collect(dgenericRowData)
                  val igenericRowData = new GenericRowData(4)
                  igenericRowData.setRowKind(RowKind.INSERT)
                  igenericRowData.setField(0, value._1)
                  igenericRowData.setField(1, value._2)
                  igenericRowData.setField(2, value._3)
                  igenericRowData.setField(3, nextLbsState._2.max)
                  out.collect(igenericRowData)

                  if (nextTimeSegmentKey.hasUpperBound) {
                    range.put(Range.closedOpen(value._3, nextTimeSegmentKey.upperEndpoint()), (value._2, nextLbsState._2 + value._3))
                  } else {
                    range.put(Range.atLeast(value._3), (value._2, nextLbsState._2 + value._3))
                  }
                case _ =>
                  range.put(Range.closedOpen(value._3, nextTimeSegmentKey.lowerEndpoint()), (value._2, Set(value._3)))
                  val igenericRowData = new GenericRowData(4)
                  igenericRowData.setRowKind(RowKind.INSERT)
                  igenericRowData.setField(0, value._1)
                  igenericRowData.setField(1, value._2)
                  igenericRowData.setField(2, value._3)
                  igenericRowData.setField(3, value._3)
                  out.collect(igenericRowData)
              }
          }
        }

        def newSegment(value: (String, String, Long), out: Collector[RowData], range: TreeRangeMap[JLong, (String, Set[Long])]): Unit = {
          range.put(Range.atLeast(value._3), (value._2, Set(value._3)))
          val igenericRowData = new GenericRowData(4)
          igenericRowData.setRowKind(RowKind.INSERT)
          igenericRowData.setField(0, value._1)
          igenericRowData.setField(1, value._2)
          igenericRowData.setField(2, value._3)
          igenericRowData.setField(3, value._3)
          out.collect(igenericRowData)
        }

        def updateSegment(value: (String, String, Long), out: Collector[RowData], range: TreeRangeMap[JLong, (String, Set[Long])], segment: JMap.Entry[Range[JLong], (String, Set[Long])]): Unit = {
          range.put(segment.getKey, (segment.getValue._1, segment.getValue._2 + value._3))
          val ubgenericRowData = new GenericRowData(4)
          ubgenericRowData.setRowKind(RowKind.UPDATE_BEFORE)
          ubgenericRowData.setField(0, value._1)
          ubgenericRowData.setField(1, value._2)
          ubgenericRowData.setField(2, segment.getKey.lowerEndpoint)
          ubgenericRowData.setField(3, segment.getValue._2.max)
          out.collect(ubgenericRowData)

          val uagenericRowData = new GenericRowData(4)
          uagenericRowData.setRowKind(RowKind.UPDATE_AFTER)
          uagenericRowData.setField(0, value._1)
          uagenericRowData.setField(1, value._2)
          uagenericRowData.setField(2, segment.getKey.lowerEndpoint)
          uagenericRowData.setField(3, value._3)
          out.collect(uagenericRowData)
        }

        def reduceSegment(value: (String, String, Long), out: Collector[RowData], range: TreeRangeMap[JLong, (String, Set[Long])], segment: JMap.Entry[Range[JLong], (String, Set[Long])]): Unit = {
          range.put(Range.closedOpen(segment.getKey.lowerEndpoint(), value._3), ("", Set()))

        }

        def splitAfterSegment(): Unit = {

        }
      })

    val hudiProps = DFSPropertiesConfiguration.getGlobalProps
    val hudiConf = Configuration.fromMap(hudiProps.asInstanceOf[JMap[String, String]])
    flinkConf.addAll(hudiConf)

    OptionsInference.setupSinkTasks(flinkConf, env.getParallelism)

    val rowType = RowType.of(false, Array(new VarCharType, new VarCharType, new BigIntType, new BigIntType), Array("mdn", "cell_key", "start_time", "end_time"))
    flinkConf.setString(FlinkOptions.SOURCE_AVRO_SCHEMA, AvroSchemaConverter.convertToSchema(rowType).toString)

    flinkConf.setString(FlinkOptions.PATH, "file:/tmp/t1")
    flinkConf.setString(FlinkOptions.TABLE_TYPE, FlinkOptions.TABLE_TYPE_MERGE_ON_READ)
    flinkConf.setString(FlinkOptions.TABLE_NAME, "t1")
    flinkConf.setString(FlinkOptions.PRECOMBINE_FIELD, "end_time")
    flinkConf.setString(FlinkOptions.RECORD_KEY_FIELD, "mdn")

    flinkConf.setBoolean(FlinkOptions.HIVE_STYLE_PARTITIONING, true)
    flinkConf.setString(FlinkOptions.KEYGEN_TYPE, KeyGeneratorType.NON_PARTITION.name)

    flinkConf.setString(FlinkOptions.COMPACTION_TRIGGER_STRATEGY, FlinkOptions.TIME_ELAPSED)
    flinkConf.setInteger(FlinkOptions.COMPACTION_DELTA_SECONDS, TimeUnit.SECONDS.toSeconds(120).toInt)

    val hoodieRecordDataStream = Pipelines.bootstrap(flinkConf, rowType, dataStream.javaStream)
    val pipeline = Pipelines.hoodieStreamWrite(flinkConf, hoodieRecordDataStream)
    Pipelines.compact(flinkConf, pipeline)

    env.execute()
  }
}
