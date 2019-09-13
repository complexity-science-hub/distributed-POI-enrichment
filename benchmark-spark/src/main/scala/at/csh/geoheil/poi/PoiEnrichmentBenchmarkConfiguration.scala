// Copyright (C) 2019 Georg Heiler
package at.csh.geoheil.poi

import at.csh.geoheil.common.config.{
  BaseConfiguration,
  DataSourceSinglePartition,
  OSMRawConfiguration
}

final case class POIC(osmFilterTags: Map[String, Seq[String]],
                      osmAdditionalColumns: Map[String, String])

final case class PoiBenchmark(usersMax: Long,
                              periods: Int,
                              yearMin: Int,
                              yearMax: Int,
                              benchmarkRuns: Int,
                              baseRadiusMin: Double,
                              baseRadiusMax: Double,
                              baseNumberOfEvents: Long,
                              boundingWKTGeometry: String)

case class PoiEnrichmentBenchmarkConfiguration(
    override val applicationName: String,
    override val osmNode: DataSourceSinglePartition,
    override val osmWay: DataSourceSinglePartition,
    override val osmRelation: DataSourceSinglePartition,
    poiEnrichment: POIC,
    poiBenchmark: PoiBenchmark
) extends BaseConfiguration
    with OSMRawConfiguration
