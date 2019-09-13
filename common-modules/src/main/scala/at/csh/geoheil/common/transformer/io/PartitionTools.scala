// Copyright (C) 2019 Georg Heiler
package at.csh.geoheil.common.transformer.io

import at.csh.geoheil.common.config.{DataSource, DataSourceSinglePartition}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.max

object PartitionTools {
  // TODO consider to refactor into less copy paste code

  /**
    * Filters a spark Dataset[T] to either most upd to date partition (no configuration specified)
    * selected partition (must be specified) Or interval (select partition and interval are XOR.
    *
    * @param partitionColumn column name of partitioning column
    * @param ds              DataSource with desired configuration
    * @param df              Dataset to be filtered
    * @tparam T
    * @return Dataset[T] filtered according to configuration of ds
    */
  def filterPartition[T](partitionColumn: String, ds: DataSource)(
      df: Dataset[T]): Dataset[T] =
    ds match {
      // TODO consider to refactor and use https://github.com/fthomas/refined
      case singlePartition: DataSourceSinglePartition =>
        handleFilterDsToSinglePartition(partitionColumn,
                                        df,
                                        singlePartition.partitionValue)
      case _ =>
        throw new NotImplementedError("this datasource was not yet implemented")
    }

  private def handleFilterDsToSinglePartition[T](partitionColumn: String,
                                                 df: Dataset[T],
                                                 pv: Option[String]) = {
    df.filter(
      col(partitionColumn) === getPartitionValue(partitionColumn, pv, df.toDF))
  }

  def getPartitionValue(partitionColumn: String,
                        partitionValue: Option[String],
                        df: DataFrame): String =
    handleSinglePartition(partitionColumn, df, partitionValue)

  private def handleSinglePartition(partitionColumn: String,
                                    df: DataFrame,
                                    partitionValue: Option[String]) = {
    partitionValue match {
      case Some(p) => p
      case None    => getMaxValue(df, partitionColumn)
    }
  }

  private def getMaxValue(df: DataFrame, partitionColumn: String): String = {
    import df.sparkSession.implicits._
    df.select(max(col(partitionColumn))).as[String].first
  }
}
