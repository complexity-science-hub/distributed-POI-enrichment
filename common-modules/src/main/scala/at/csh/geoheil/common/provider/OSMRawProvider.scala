// Copyright (C) 2019 Georg Heiler
package at.csh.geoheil.common.provider

import at.csh.geoheil.common.config.OSMRawConfiguration
import at.csh.geoheil.common.model.raw.{OSMNode, OSMRelation, OSMWay}
import at.csh.geoheil.common.transformer.io.{IO, PartitionTools}
import org.apache.spark.sql.{DataFrame, SparkSession}

object OSMRawProvider {

  def provideAndFilter(c: OSMRawConfiguration)(
      implicit spark: SparkSession): (DataFrame, DataFrame, DataFrame) = {
    val (node, way, relation) = provide(c)
    val nodeFiltered = node.transform(
      PartitionTools.filterPartition(OSMNode.outputPartitionCol, c.osmNode))
    val wayFiltered = way.transform(
      PartitionTools.filterPartition(OSMWay.outputPartitionCol, c.osmWay))
    val relationFiltered = relation.transform(
      PartitionTools.filterPartition(OSMRelation.outputPartitionCol,
                                     c.osmRelation))
    (nodeFiltered, wayFiltered, relationFiltered)
  }

  def provide(c: OSMRawConfiguration)(
      implicit spark: SparkSession): (DataFrame, DataFrame, DataFrame) = {
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
        OSMNode.yLatWgs84,
        OSMNode.outputPartitionCol
      )

    val way = IO
      .readPrefixedParquetFile(c.osmWay)
      .select(
        OSMWay.id,
        OSMWay.version,
        OSMWay.timestamp,
        OSMWay.tags,
        OSMWay.nodes,
        OSMWay.outputPartitionCol
      )

    val relation = IO
      .readPrefixedParquetFile(c.osmRelation)
      .select(
        OSMRelation.id,
        OSMRelation.version,
        OSMRelation.timestamp,
        OSMRelation.tags,
        OSMRelation.members,
        OSMRelation.outputPartitionCol
      )
    (node, way, relation)
  }
}
