// Copyright (C) 2019 Georg Heiler
package at.csh.geoheil.common.model.raw

import at.csh.geoheil.common.config.HasOutputPartition

object OSMWay extends HasOutputPartition {
  override val outputPartitionCol: String =
    FeedIdentifierColumns.outputPartitionCol
  val id = "id"
  val version = "version"
  val timestamp = "timestamp"
  val tags = "tags"
  val nodes = "nodes"
  val index = "index"
  val nodeId = "nodeId"
}
