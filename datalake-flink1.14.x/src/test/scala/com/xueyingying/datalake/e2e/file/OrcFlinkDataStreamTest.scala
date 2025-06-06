package com.xueyingying.datalake.e2e.file

import java.nio.charset.StandardCharsets
import java.util.Properties

import com.xueyingying.datalake.FlinkSuiteBase
import org.apache.flink.connector.file.sink.FileSink
import org.apache.flink.core.fs.Path
import org.apache.flink.core.io.SimpleVersionedSerializer
import org.apache.flink.orc.vector.Vectorizer
import org.apache.flink.orc.writer.OrcBulkWriterFactory
import org.apache.flink.runtime.util.HadoopUtils
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer
import org.apache.flink.table.filesystem.RowPartitionComputer
import org.apache.flink.table.utils.PartitionPathUtils
import org.apache.flink.types.Row
import org.apache.hadoop.hive.ql.exec.vector.{BytesColumnVector, LongColumnVector, VectorizedRowBatch}
import org.apache.orc.TypeDescription

import scala.collection.JavaConversions.propertiesAsScalaMap

/**
 * @author cx330.1000ly@gmail.com
 * @version 1.0.0
 * @since 2023-01-10
 */
class OrcFlinkDataStreamTest extends FlinkSuiteBase {
  "orc" should "write" in {
    val schema: TypeDescription = TypeDescription.createStruct()
      .addField("id", TypeDescription.createLong())
      .addField("content", TypeDescription.createString())
    val writerProperties = new Properties()
    writerProperties += ("orc.compress" -> "LZ4")
    val sink: FileSink[Row] = FileSink
      .forBulkFormat(new Path("/tmp/t1"), new OrcBulkWriterFactory(new Vectorizer[Row](schema.toString) {
        override def vectorize(element: Row, batch: VectorizedRowBatch): Unit = {
          val idColVector = batch.cols(0).asInstanceOf[LongColumnVector]
          val contentColVector = batch.cols(1).asInstanceOf[BytesColumnVector]

          idColVector.vector(batch.size + 1) = element.getField(0).asInstanceOf[Long]
          contentColVector.setVal(batch.size + 1, element.getField(1).asInstanceOf[String].getBytes(StandardCharsets.UTF_8))
        }
      }, writerProperties, HadoopUtils.getHadoopConfiguration(flinkConf)))
      .withBucketAssigner(new BucketAssigner[Row, String] {
        val computer = new RowPartitionComputer("000000", Array("id", "content"), Array("id"))

        override def getBucketId(element: Row, context: BucketAssigner.Context): String = PartitionPathUtils.generatePartitionPath(computer.generatePartValues(element))

        override def getSerializer: SimpleVersionedSerializer[String] = SimpleVersionedStringSerializer.INSTANCE
      })
      .build()

    rowStream.sinkTo(sink)

    env.execute
  }
}
