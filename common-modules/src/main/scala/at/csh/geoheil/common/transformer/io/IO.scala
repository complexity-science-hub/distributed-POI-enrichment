// Copyright (C) 2019 Georg Heiler
package at.csh.geoheil.common.transformer.io

import at.csh.geoheil.common.config.PrefixedData
import at.csh.geoheil.common.transformer.cleanup.Renamer
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object IO {

  def readPrefixedParquetFile(prefixedData: PrefixedData)(
      implicit spark: SparkSession): DataFrame = {
    spark.read.parquet(s"${prefixedData.prefix}/${prefixedData.fileName}")
  }

  def writeParquet(df: DataFrame,
                   path: String,
                   saveMode: SaveMode,
                   partitionColumn: Option[Seq[String]] = None): Unit =
    writerCommon(df, saveMode, partitionColumn)
      .option("compression", "gzip")
      .parquet(path)

  private def writerCommon(df: DataFrame,
                           saveMode: SaveMode,
                           partitionColumn: Option[Seq[String]]) = {
    val renamed = df.transform(Renamer.renameDFtoLowerCase())
    val partitioned = partitionColumn match {
      case Some(s) => renamed.write.partitionBy(s: _*)
      case None    => renamed.write
    }
    partitioned.mode(saveMode)
  }
}
