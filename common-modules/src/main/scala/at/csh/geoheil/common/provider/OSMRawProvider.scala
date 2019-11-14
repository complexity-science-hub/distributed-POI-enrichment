// Copyright (C) 2019 Georg Heiler
package at.csh.geoheil.common.provider

import at.csh.geoheil.common.config.OSMRawConfiguration
import at.csh.geoheil.common.model.raw.{OSMNode, OSMRelation, OSMWay}
import at.csh.geoheil.common.transformer.io.{IO, PartitionTools}
import org.apache.spark.sql.{DataFrame, SparkSession}

object OSMRawProvider {

  def provideAndFilter(c: OSMRawConfiguration)(
      implicit spark: SparkSession): (DataFrame) = {
    val (node) = provide(c)
    val nodeFiltered = node.transform(
      PartitionTools.filterPartition(OSMNode.outputPartitionCol, c.osmNode))
    (nodeFiltered)
  }

  def provide(c: OSMRawConfiguration)(
      implicit spark: SparkSession): DataFrame = {
    val node = IO
      .readPrefixedParquetFile(c.osmNode)
      .withColumnRenamed("latitude", OSMNode.yLatWgs84)
      .withColumnRenamed("longitude", OSMNode.xLongWgs84)
      .select(
        OSMNode.id,
        OSMNode.version,
        OSMNode.timestamp,
        OSMNode.tags,
        OSMNode.xLongWgs84,
        OSMNode.yLatWgs84
      )
    node
  }
}
