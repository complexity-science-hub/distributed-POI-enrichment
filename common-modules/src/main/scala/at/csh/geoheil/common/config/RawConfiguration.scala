// Copyright (C) 2019 Georg Heiler
package at.csh.geoheil.common.config

trait OSMRawConfiguration {
  def osmNode: DataSourceSinglePartition
  def osmWay: DataSourceSinglePartition
  def osmRelation: DataSourceSinglePartition
}
