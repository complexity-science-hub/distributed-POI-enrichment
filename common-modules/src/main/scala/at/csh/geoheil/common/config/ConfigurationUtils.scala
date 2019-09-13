// Copyright (C) 2019 Georg Heiler
package at.csh.geoheil.common.config

import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import pureconfig.ConfigReader

object ConfigurationUtils {

  val defaultTimeZone = "UTC"

  def createSparkSession(sparkConf: SparkConf): SparkSession = {
    if (sparkConf.get("spark.app.name") equals "dev") {
      // https://stackoverflow.com/questions/48008343/sbt-test-does-not-work-for-spark-test
      // https://builds.apache.org/job/Derby-docs/lastSuccessfulBuild/artifact/trunk/out/security/csecjavasecurity.html
      // https://issues.apache.org/jira/browse/SPARK-22918
      System.setSecurityManager(null)
    }
    SparkSession
      .builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()
  }

  def createConf(appName: String, useKryo: Boolean): SparkConf = {
    val baseConf = new SparkConf()
      .setAppName(appName)
      .setIfMissing("spark.master", "local[5]")
      .setIfMissing("spark.driver.memory", "2G")
      //      .setIfMissing("spark.speculation", "true")
      //      .setIfMissing("spark.driver.maxResultSize", "2G")
      .setIfMissing("spark.sql.session.timeZone", defaultTimeZone)
      .setIfMissing("spark.sql.autoBroadcastJoinThreshold", "1572864000")
      .setIfMissing("spark.executor.cores", "4")
      .setIfMissing("spark.eventLog.compress", "true")

      // turn on Parquet push-down, stats filtering, and dictionary filtering
      .setIfMissing("parquet.filter.dictionary.enabled", "true")
      // https://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.6.4/bk_spark-component-guide/content/orc-spark.html
      .setIfMissing("spark.sql.orc.impl", "native")
      .setIfMissing("spark.sql.orc.enabled", "true")
      .setIfMissing("spark.sql.orc.enableVectorizedReader", "true")
      .setIfMissing("spark.sql.hive.convertMetastoreOrc", "true")
      .setIfMissing("spark.sql.orc.filterPushdown", "true")
      .setIfMissing("spark.sql.orc.char.enabled", "true")

      // cost based optimizer https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-cost-based-optimization.html
      .setIfMissing("spark.sql.cbo.enabled", "true")
      .setIfMissing("spark.sql.statistics.histogram.enabled", "true")

      // adaptive optimization at runtime
      .setIfMissing("spark.sql.adaptive.enabled", "true")
      // TODO set when https://github.com/Intel-bigdata/spark-adaptive has been merged into main spark
      //.setIfMissing("spark.sql.adaptive.skewedJoin.enabled", "true")

      // Set hive options in spark to prevent files which are too small
      // https://stackoverflow.com/questions/48202486/best-practice-merging-files-after-spark-batch-job
      .setIfMissing("spark.hadoop.hive.merge.sparkfiles", "true")
      .setIfMissing("spark.hadoop.hive.merge.mapredfiles", "true")
      .setIfMissing("spark.hadoop.hive.merge.mapfiles", "true")
      // should be to HDFS_BLOCKSIZE, TODO read dynamically
      // spark.conf.getOption("dfs.block.size", "128000000")
      .setIfMissing("spark.hadoop.hive.merge.smallfiles.avgsize", "128000000")
      .setIfMissing("spark.hadoop.hive.merge.size.per.task", "128000000")

    if (useKryo) {
      baseConf
        .setIfMissing("spark.serializer",
                      classOf[KryoSerializer].getCanonicalName)
        .setIfMissing("spark.kryoserializer.buffer.max", "256m")
        .setIfMissing("spark.kryo.registrationRequired", "true")
        .setIfMissing("spark.kryo.unsafe", "true")
        .setIfMissing("spark.kryo.referenceTracking", "false")
        .setIfMissing("spark.kryo.registrator",
                      classOf[CommonKryoRegistrator].getName)
    } else {
      baseConf
    }
  }

  def loadConfiguration[T <: Product: ConfigReader]: T = {
    val r = pureconfig.loadConfig[T]
    r match {
      case Right(s) => s
      case Left(l) =>
        throw new ConfigurationException(
          s"Failed to start. There is a problem with the configuration: ${l}"
        )
    }
  }
}
