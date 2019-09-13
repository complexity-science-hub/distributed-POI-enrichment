// Copyright (C) 2019 Georg Heiler
package at.csh.geoheil.common.model.raw

import at.csh.geoheil.common.config.HasOutputPartition

object OSMRelation extends HasOutputPartition {
  val id = "id"
  val version = "version"
  val timestamp = "timestamp"
  val tags = "tags"
  val members = "members"
  val role = "role"
  val kind = "type"
  override val outputPartitionCol: String =
    FeedIdentifierColumns.outputPartitionCol
}
