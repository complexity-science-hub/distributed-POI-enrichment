// Copyright (C) 2019 Georg Heiler
package at.csh.geoheil.common

import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK

object SparkUtils {

  def setShuffleParallelism(parallelism: Int)(
      implicit spark: SparkSession): Unit =
    setSparkConfig("spark.sql.shuffle.partitions", parallelism.toString)

  def setSparkConfig(key: String, value: String)(
      implicit spark: SparkSession): Unit = {
    spark.conf.set(key, value)
  }

  /**
    * Use catalyst to apply a column / UDF only when it is not null
    * @param column column which is potentially null
    * @param inputCols inputs for the transformation for the column above
    * @return columns contents or null
    */
  def nullableFunction(column: Column, inputCols: String*): Column = {
    when(inputCols.map(col).map(_.isNotNull).reduce(_ and _), column)
  }

  /**
    * Display user friendly names of cached table in Spark web UI storage tab to ease debugging.
    * https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-caching-webui-storage.html
    * @param name name of the table
    * @param storageLevel regular spark storage level
    * @param df input to be cached
    * @return the lazily cached df for further processing
    */
  def namedCache(name: String, storageLevel: StorageLevel = MEMORY_AND_DISK)(
      df: DataFrame): DataFrame = {
    df.sparkSession.sharedState.cacheManager
      .cacheQuery(df, Some(name), storageLevel)
    df
  }

}
