package com.xueyingying.datalake

import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.configuration.{Configuration, CoreOptions, GlobalConfiguration, JobManagerOptions, RestOptions, SecurityOptions, StateBackendOptions, TaskManagerOptions, WebOptions}
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions
import org.apache.flink.streaming.api.functions.source.datagen.{DataGenerator, DataGeneratorSource}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

/**
 * @author cx330.1000ly@gmail.com
 * @version 1.0.0
 * @since 2023-01-09
 */
trait FlinkSuiteBase extends AnyFlatSpec with BeforeAndAfterAll {
  @transient var flinkConf: Configuration = _
  @transient var env: StreamExecutionEnvironment = _

  val HIVE_CATALOG_NAME = "hive_catalog"
  val HUDI_CATALOG_NAME = "hoodie_catalog"

  override def beforeAll: Unit = {
    super.beforeAll

    flinkConf = GlobalConfiguration.loadConfiguration
    flinkConf.set(RestOptions.ENABLE_FLAMEGRAPH, Boolean.box(true))
    sys.props("os.name") match {
      case os if os.startsWith("Mac OS X") =>
        flinkConf.set(CoreOptions.TMP_DIRS, "/private/tmp")
      case _ =>
        flinkConf.set(CoreOptions.TMP_DIRS, "/tmp")
    }
    flinkConf.set(WebOptions.TMP_DIR, "/tmp")
    flinkConf.set(TaskManagerOptions.NUM_TASK_SLOTS, Int.box(50))
    // flinkConf.setBoolean(PipelineOptions.GENERIC_TYPES, false)
    flinkConf.set(StateBackendOptions.LATENCY_TRACK_ENABLED, Boolean.box(true))
    flinkConf.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, Boolean.box(true))

    enableSSL(flinkConf)
    enableMetrics(flinkConf)
    // enableHistory(flinkConf)

    env = StreamExecutionEnvironment.getExecutionEnvironment
    env.disableOperatorChaining

    enableCheckpoint(env)

    val source = new DataGeneratorSource[String]("", 1, null)

    env.addSource()
  }

  def enableSSL(flinkConfig: Configuration): Unit = {
    val keystore = getClass.getClassLoader.getResource("flink.jks").getPath
    flinkConfig.set(SecurityOptions.SSL_REST_ENABLED, Boolean.box(true))
    flinkConfig.set(SecurityOptions.SSL_REST_KEYSTORE, keystore)
    flinkConfig.set(SecurityOptions.SSL_REST_KEYSTORE_PASSWORD, "flink123")
    flinkConfig.set(SecurityOptions.SSL_REST_KEY_PASSWORD, "flink123")
  }

  def enableCheckpoint(env: StreamExecutionEnvironment): Unit = {
    // val stateBackend = new EmbeddedRocksDBStateBackend(true)
    val stateBackend = new HashMapStateBackend()
    env.setStateBackend(stateBackend)
    env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE)
    // env.getCheckpointConfig.setCheckpointStorage("hdfs://hacluster/tmp/flink/ckp")
    env.getCheckpointConfig.setCheckpointTimeout(30000)
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
  }

  def enableMetrics(flinkConfig: Configuration): Unit = {
    flinkConfig.setString("metrics.reporter.promgateway.class", "org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporter")
    flinkConfig.setString("metrics.reporter.promgateway.host", "xueyingying.com")
    flinkConfig.setString("metrics.reporter.promgateway.port", "9091")
    flinkConfig.setString("metrics.reporter.promgateway.deleteOnShutdown", "true")
  }

  def enableHistory(flinkConfig: Configuration): Unit = {
    flinkConfig.set(JobManagerOptions.ARCHIVE_DIR, "hdfs://hacluster/flink-history")
  }

  override def afterAll: Unit = {}
}
