// Copyright (C) 2019 Georg Heiler
package at.csh.geoheil.poi

import java.sql.Timestamp

import at.csh.geoheil.common.model.raw.OSMNode
import at.csh.geoheil.common.transformer.geometry.GeometryService
import at.csh.geoheil.common.transformer.io.IO
import at.csh.geoheil.common.transformer.{SqlTools, StructTypeHelpers}
import org.apache.calcite.sql.JoinType
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{
  col,
  collect_list,
  sort_array,
  struct,
  udf
}
import org.apache.spark.sql.types.DoubleType
import org.locationtech.geomesa.spark.GeometricDistanceFunctions
import org.locationtech.geomesa.spark.jts.util.WKTUtils
import org.locationtech.jts.geom.{Coordinate, GeometryFactory}

case class OsmParsedSpecific(id: Long,
                             x_long_wgs84: Double,
                             y_lat_wgs84: Double,
                             name: String,
                             amenity: Option[String],
                             cuisine: Option[String],
                             speciality: Option[String],
                             emergency: Option[String],
                             healthcare: Option[String])
case class OsmParsedSpecificGeometry(
    id: Long,
    geometry: org.locationtech.jts.geom.Geometry,
    name: String,
    amenity: Option[String],
    cuisine: Option[String],
    speciality: Option[String],
    emergency: Option[String],
    healthcare: Option[String])

case class ClosePoi(id: Long,
                    distance: Double,
                    name: String,
                    amenity: Option[String],
                    cuisine: Option[String],
                    speciality: Option[String],
                    emergency: Option[String],
                    healthcare: Option[String])

case class EnrichedEvent(
    // TODO change class hierarchy, define shared interface for basic parts of event
    event_time: Timestamp,
    x_long: Double,
    y_lat: Double,
    area: String,
    close_pois: Seq[ClosePoi]
)

object PoiEnrichmentBenchmarkService {
  @transient
  private lazy val geomFactory: GeometryFactory = new GeometryFactory()
  private val BENCHMARK_OUT_FILE = "tmp_benchmark/bench_out.parquet"

  def distributedPOIEnrichmentWithExplode(
      events: DataFrame,
      desiredNodes: DataFrame,
      additionalConfig: PoiBenchmark): Unit = {
    val osmGeometryName = "osm_node_geometry"
    val eventGeometryName = "event_geometry"

    val eventsAsRows = events
      .transform(SqlTools.explodeDf(UserSpecificEvent.events))
      .transform(
        StructTypeHelpers.unpackSingleLevelStruct(UserSpecificEvent.events))
      .transform(GeometryService.parseWktToGeoSparkGeometry(Event.area,
                                                            eventGeometryName))

    // spatial join
    // 1) distance join (not implemented, as simple distance not meaningful in 4326 and radians would need to be converted.
    // 2) intersects with event area
    val osmRXLong = s"osm_${OSMNode.xLongWgs84}"
    val osmRYLat = s"osm_${OSMNode.yLatWgs84}"
    val osmId = s"osm_${OSMNode.id}"
    val desiredNodesWithGeometry = desiredNodes
    // TODO evaluate and figure out right level
    // increase parallelism from 10 to 100
      .repartition(50)
      .transform(
        GeometryService.parseXYToGeosparkPointGeometry(OSMNode.xLongWgs84,
                                                       OSMNode.yLatWgs84,
                                                       osmGeometryName))
      .withColumnRenamed(OSMNode.xLongWgs84, osmRXLong)
      .withColumnRenamed(OSMNode.yLatWgs84, osmRYLat)
      .withColumnRenamed(OSMNode.id, osmId)

    val enrichedResults = GeometryService
      .filterSpaceVector(eventsAsRows,
                         eventGeometryName,
                         desiredNodesWithGeometry,
                         osmGeometryName)
      .drop(eventGeometryName, osmGeometryName)

    // TODO move to proper LEFT JOIN strategy directly inside spatial join
    // when https://github.com/DataSystemsLab/GeoSpark/pull/255 is merged
    val a = eventsAsRows.drop(eventGeometryName)
    val leftJoinedResult =
      eventsAsRows.join(enrichedResults, a.columns, JoinType.LEFT.name)

    // debugging visualization for https://kepler.gl
    // IO.writeCsv(leftJoinedResult.drop("event_point_geometry").repartition(1),
    //              "kepler.csv",
    //              ",",
    //              SaveMode.Overwrite)

    // aggregate to stays again and keep a list of POI entries
    val matchedPoisName = "matched_pois"
    val distanceToPoi = "distance_to_poi"
    val columnsInStruct = desiredNodesWithGeometry
      .drop(osmGeometryName)
      .columns ++ Array(distanceToPoi)
    val poiEnriched = leftJoinedResult
      .transform(
        GeometryService.parseXYToGeomesaPointGeometry(osmRXLong,
                                                      osmRYLat,
                                                      osmGeometryName))
      .transform(
        GeometryService.parseXYToGeomesaPointGeometry(Event.x_long,
                                                      Event.y_lat,
                                                      eventGeometryName))
      .withColumn(osmRXLong, col(osmRXLong).cast(DoubleType))
      .withColumn(osmRYLat, col(osmRYLat).cast(DoubleType))
      .withColumn(distanceToPoi,
                  SqlTools.geomesa_ST_DistanceSpheroid(col(eventGeometryName),
                                                       col(osmGeometryName)))
      .groupBy(a.columns.map(col): _*)
      .agg(collect_list(struct(columnsInStruct.map(col): _*))
        .alias(matchedPoisName))
      // re-aggregate to enriched data locality valuing chain
      .groupBy(UserSpecificEvent.id, UserSpecificEvent.periods)
      .agg(
        sort_array(
          collect_list(
            struct(
              col(Event.event_time),
              col(Event.x_long),
              col(Event.y_lat),
              col(Event.area),
              col(matchedPoisName)
            ))).alias(UserSpecificEvent.events))

    IO.writeParquet(
      poiEnriched /*.orderBy(PoiColumnNames.id)*/, // Not validating ordering
      BENCHMARK_OUT_FILE,
      SaveMode.Overwrite,
      Some(Seq(UserSpecificEvent.periods))
    )
  }

  def distributedPOIEnrichmentAlreadyExplodedNoLocalityPreservedOnlyInnerJoin(
      dummyDataRaw: DataFrame,
      desiredNodes: DataFrame,
      additionalConfig: PoiBenchmark): Unit = {
    val osmGeometryName = "osm_node_geometry"
    val eventGeometryName = "event_geometry"

    val eventsAsRows = dummyDataRaw
      .transform(
        GeometryService.parseWktToGeoSparkGeometry(Event.area,
                                                   eventGeometryName))

    // spatial join
    // 1) distance join (not implemented, as simple distance not meaningful in 4326 and radians would need to be converted.
    // 2) intersects with event area
    val osmRXLong = s"osm_${OSMNode.xLongWgs84}"
    val osmRYLat = s"osm_${OSMNode.yLatWgs84}"
    val osmId = s"osm_${OSMNode.id}"
    val desiredNodesWithGeometry = desiredNodes
    // TODO evaluate and figure out right level
    // increase parallelism from 10 to 100
      .repartition(50)
      .transform(
        GeometryService.parseXYToGeosparkPointGeometry(OSMNode.xLongWgs84,
                                                       OSMNode.yLatWgs84,
                                                       osmGeometryName))
      .withColumnRenamed(OSMNode.xLongWgs84, osmRXLong)
      .withColumnRenamed(OSMNode.yLatWgs84, osmRYLat)
      .withColumnRenamed(OSMNode.id, osmId)

    val enrichedResults = GeometryService
      .filterSpaceVector(eventsAsRows,
                         eventGeometryName,
                         desiredNodesWithGeometry,
                         osmGeometryName)
      .drop(eventGeometryName, osmGeometryName)

    IO.writeParquet(
      enrichedResults.drop(eventGeometryName),
      BENCHMARK_OUT_FILE,
      SaveMode.Overwrite,
      Some(Seq(UserSpecificEvent.periods))
    )
  }

  def dataLocalityPreservingPOIEnrichment(
      tripChain: DataFrame,
      desiredNodes: DataFrame,
      additionalConfig: PoiBenchmark)(implicit spark: SparkSession): Unit = {
    // load into local memory. WARNING: this works fine for Austria, but might fail in case of US i.e. very large number of points
    // this class is specific to the paramters inputted when selecting the nodes. maybe it could be generated using meta programming. Using a class like this greatly eases the access i.e. no RDD but rather Dataset[T]
    // TODO figure out how to make it more transparent / not required to use the specific class
    import spark.implicits._
    val localNodes = desiredNodes.as[OsmParsedSpecific].collect
//    println(s"local nodes: ${localNodes.size}")

    import com.github.plokhotnyuk.rtree2d.core._
    import SphericalEarth._

    // add points to spatial index
    // TOdO experiment with spark driver cores parallelism
    val geometryNodes = localNodes.map(
      e =>
        entry(
          e.x_long_wgs84.toFloat,
          e.y_lat_wgs84.toFloat,
          OsmParsedSpecificGeometry(
            e.id,
            geomFactory.createPoint(
              new Coordinate(e.x_long_wgs84, e.y_lat_wgs84)),
            e.name,
            e.amenity,
            e.cuisine,
            e.speciality,
            e.emergency,
            e.healthcare)
      ))
//    println("created geometries")
    // TODO tune Rtree Node capacity. STR(JTS) default capacity is 10. Geospark uses: samples.size() / partitions, here 16 is used.
    val spatialIndex = RTree(geometryNodes, 8)

//    println("created spatial index")

    // broadcast to executor nodes
    val broadcastedIndex = spark.sparkContext.broadcast(spatialIndex)

//    println("broadcasted")

    def performLocalityPreservingSpatialJoin(
        events: Seq[Event]): Seq[EnrichedEvent] = {
      // iterate through the events in same memory region.
      events.map(e => {
        val geometry = WKTUtils.read(e.area)
        val envelope = geometry.getEnvelopeInternal
        val candidates =
          // query the MBR rectangle from the spatial index
          broadcastedIndex.value.searchAll(envelope.getMinX.toFloat,
                                           envelope.getMinY.toFloat,
                                           envelope.getMaxX.toFloat,
                                           envelope.getMaxY.toFloat)

        // for true candidates perform intersection test and calculate distances
        val trueMatches: Seq[OsmParsedSpecificGeometry] =
          candidates.map(_.value).filter(tm => geometry.intersects(tm.geometry))
        val matchedPOI = if (trueMatches.isEmpty) {
          Seq()
        } else {
          // calculate distances
          trueMatches.map(
            tm =>
              ClosePoi(
                tm.id,
                GeometricDistanceFunctions.fastDistance(
                  new Coordinate(e.x_long, e.y_lat),
                  tm.geometry.getCoordinate),
                tm.name,
                tm.amenity,
                tm.cuisine,
                tm.speciality,
                tm.emergency,
                tm.healthcare
            ))
        }
        EnrichedEvent(e.event_time, e.x_long, e.y_lat, e.area, matchedPOI)
      })
    }
    def poiEnrichment(rawChain: Seq[Row]): Seq[EnrichedEvent] = {
      performLocalityPreservingSpatialJoin(rawChain.map(Event.fromRow))
    }
    val localityPreservingPOIEnrichmentUDF = udf(poiEnrichment _)
    // geospark is exploding = repartitioning the dataset. this gives it an unfair advantage
    // need to correct here so that more partitions are loaded by default for broadcast spatial join
    // but correct it in memory!!!
    val first = userLoad * 300
    val second = userLoad * 30
    val
    val poiEnriched = tripChain.repartition(user_load * 300).withColumn(
      "enhanced",
      localityPreservingPOIEnrichmentUDF(col(UserSpecificEvent.events)))
    //    poiEnriched.show
    IO.writeParquet(
      poiEnriched /*.orderBy(UserSpecificEvent.id)*/, // sorting is not tested for now
      BENCHMARK_OUT_FILE,
      SaveMode.Overwrite,
      Some(Seq(UserSpecificEvent.periods))
    )
  }

  def time[R](upToKTimes: Int,
              resultTimingKey: String,
              block: => R): Seq[TimingResult] = {

    val kTimes = Range(1, upToKTimes + 1)
    for (k <- kTimes)
      yield {
        val t0 = System.nanoTime()
        /*val result = */
        block // call-by-name
        val t1 = System.nanoTime()
        val elapsed = t1 - t0
        val elapsedSeconds = elapsed / 1000000000.0
//        println("########### *************** ###########")
//        println("Elapsed time: " + elapsed + "ns")
//        println("Elapsed time: " + elapsedSeconds + "seconds")
//        println("########### *************** ###########")
        TimingResult(resultTimingKey, elapsedSeconds)
      }
  }

}
