// Copyright (C) 2019 Georg Heiler
package at.csh.geoheil.poi

import at.csh.geoheil.common.{SparkBaseRunner, SparkUtils}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import pureconfig.generic.auto._
import pureconfig.module.enumeratum._

/**
  * Ingest OSM parquet to HDFS. some cleanup is performed.
  * https://georgheiler.com/2019/05/03/osm-to-spark/
  */
object OsmDataLoader extends SparkBaseRunner[OsmLoadConfiguration] {
  implicit val spark: SparkSession = createSparkSession(
    createSparkConfiguration(c.applicationName))

  val node = readData(c.osmLoad.sharedPrefix, c.osmLoad.nodePath)
    .transform(handleCommon())
  val way = readData(c.osmLoad.sharedPrefix, c.osmLoad.wayPath)
    .transform(handleCommon())
  val relation = readData(c.osmLoad.sharedPrefix, c.osmLoad.relationPath)
    .transform(handleCommon())

  // for AT the data is rahter small, keep the number of files also smaller than the default of 200
  SparkUtils.setShuffleParallelism(10)

  cleanupAndWrite(node, c.osmLoad.nodePath)
  cleanupAndWrite(way, c.osmLoad.wayPath)
  cleanupAndWrite(relation, c.osmLoad.relationPath)

  private def cleanupAndWrite(df: DataFrame, outputFileName: String): Unit = {
    df.withColumnRenamed("latitude", "y_lat_wgs84")
      .withColumnRenamed("longitude", "x_long_wgs84")
      .orderBy("id")
      .write
      .mode(c.osmLoad.saveMode)
      .partitionBy("dt")
      .option("compression", "gzip")
      .parquet(s"${c.osmLoad.outputFolder}/$outputFileName")
  }

  private def toMap(tupesArray: Seq[Row]): Option[Map[String, String]] = {
    if (tupesArray == null) {
      None
    } else {
      val tuples = tupesArray.map(e => {
        (
          e.getAs[String]("key"),
          e.getAs[String]("value")
        )
      })
      Some(tuples.toMap)
    }
  }

  private def handleCommon()(df: DataFrame): DataFrame = {
    val toMapUDF = udf(toMap _)
    df.withColumn("dt", lit(current_date()))
      .drop("uid", "user_sid", "changeset")
      .withColumn("tags", toMapUDF(col("tags")))
      .withColumn("timestamp", from_unixtime(col("timestamp") / 1000))
  }
  private def readData(prefix: String, file: String)(
      implicit spark: SparkSession): DataFrame = {
    spark.read.parquet(s"$prefix/$file")
  }

}
