// Copyright (C) 2019 Georg Heiler
package at.csh.geoheil.common.model.raw

import at.csh.geoheil.common.config.HasOutputPartition

trait FeedIdentifierColumns extends HasOutputPartition {
  override def outputPartitionCol: String
}

object FeedIdentifierColumns extends HasOutputPartition {
  override def outputPartitionCol: String = "dt"
}
