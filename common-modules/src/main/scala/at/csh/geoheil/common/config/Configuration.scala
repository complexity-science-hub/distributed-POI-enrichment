// Copyright (C) 2019 Georg Heiler
package at.csh.geoheil.common.config

trait BaseConfiguration {
  def applicationName: String
}

sealed trait DataSource

sealed trait PrefixedData {
  def prefix: String

  def fileName: String
}

case class DataSourceSinglePartition(prefix: String,
                                     fileName: String,
                                     partitionValue: Option[String])
    extends PrefixedData
    with DataSource

case class DataSourceNoPartition(prefix: String, fileName: String)
    extends PrefixedData
    with DataSource

trait HasOutputPartition {
  def outputPartitionCol: String
}
