package com.xueyingying.datalake.e2e.paimon

import java.time.Instant

import com.xueyingying.datalake.FlinkSuiteBase
import org.apache.flink.streaming.api.scala.{DataStream, createTypeInformation}
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.data.{GenericRowData, RowData, StringData, TimestampData}
import org.apache.flink.types.{Row, RowKind}
import org.apache.flink.util.Collector
import org.apache.paimon.CoreOptions
import org.apache.paimon.catalog.CatalogContext
import org.apache.paimon.flink.LogicalTypeConversion
import org.apache.paimon.flink.sink.FlinkSinkBuilder
import org.apache.paimon.fs.hadoop.HadoopFileIO
import org.apache.paimon.fs.local.LocalFileIO
import org.apache.paimon.options.Options
import org.apache.paimon.schema.{Schema, SchemaManager}
import org.apache.paimon.table.{FileStoreTable, FileStoreTableFactory}

/**
 * @author wuzh8@asiainfo.com
 * @version 1.0.0
 * @since 2025-02-09
 */
class PaimonFlinkDataStreamTest extends FlinkSuiteBase {
  "paimon" should "write" in {
    val dataStream: DataStream[RowData] = rowStream.flatMap[RowData] { (data: Row, out: Collector[RowData]) =>
      val igenericRowData = new GenericRowData(4)
      igenericRowData.setRowKind(RowKind.INSERT)
      igenericRowData.setField(0, TimestampData.fromEpochMillis(Instant.now().getEpochSecond))
      igenericRowData.setField(1, data.getField("id"))
      igenericRowData.setField(2, StringData.fromString(data.getField("content").asInstanceOf[String]))
      igenericRowData.setField(3, StringData.fromString(data.getField("date").asInstanceOf[String]))
      out.collect(igenericRowData)
    }

    val table = createStoreTable
    val builder = new FlinkSinkBuilder(table).withInput(dataStream.javaStream)

    builder.build
    env.execute
  }

  def createStoreTable: FileStoreTable = {
    val options = new Options
    options.set(CoreOptions.PATH, "/tmp/t1")

    val catalogContext = CatalogContext.create(options)
    val fileIO = new HadoopFileIO
    fileIO.configure(catalogContext)

    val schemaManager = new SchemaManager(fileIO, CoreOptions.path(options))
    if (!schemaManager.latest().isPresent) {
      val schema = Schema.newBuilder
        .column("a", LogicalTypeConversion.toDataType(DataTypes.TIMESTAMP.getLogicalType))
        .column("b", LogicalTypeConversion.toDataType(DataTypes.BIGINT.getLogicalType))
        .column("c", LogicalTypeConversion.toDataType(DataTypes.STRING.getLogicalType))
        .column("d", LogicalTypeConversion.toDataType(DataTypes.STRING.getLogicalType))
        .build
      schemaManager.createTable(schema)
    }

    FileStoreTableFactory.create(fileIO, options)
  }
}
