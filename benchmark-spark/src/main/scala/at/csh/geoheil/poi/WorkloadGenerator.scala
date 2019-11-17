// Copyright (C) 2019 Georg Heiler
package at.csh.geoheil.poi

import java.sql.{Date, Timestamp}
import java.time.temporal.ChronoUnit
import java.time.{LocalDate, LocalTime, Month}
import java.util.concurrent.ThreadLocalRandom

import at.csh.geoheil.common.transformer.{SqlTools, StructTypeHelpers}
import at.csh.geoheil.common.transformer.io.IO
import org.apache.spark.sql.functions.{col, explode, lit, udf}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.locationtech.geomesa.spark.jts.util.WKTUtils
import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory}

import scala.collection.mutable.ArrayBuffer

object WorkloadGenerator {

  @transient private lazy val geomFactory: GeometryFactory =
    new GeometryFactory()

  private implicit def ordered: Ordering[Timestamp] = new Ordering[Timestamp] {
    def compare(x: Timestamp, y: Timestamp): Int = x compareTo y
  }

  def generateWorkload(
      c: PoiBenchmark,
      users: Long,
      DUMMY_DATA_LOCALITY_PATH_INPUT: String,
      DUMMY_DATA_RAW_INPUT: String)(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    val dummyUser = WorkloadGenerator
      .generateUserDays(users, c.yearMin, c.yearMax, c.periods)
      .toDF(UserSpecificEvent.id, UserSpecificEvent.periods)
      .select(col(UserSpecificEvent.id),
              explode(col(UserSpecificEvent.periods))
                .alias(UserSpecificEvent.periods))
    val enrichedWithEvents =
      dummyUser.transform(WorkloadGenerator.enrichEvents(c))
    // geospark is exploding = repartitioning the dataset. this gives it an unfair advantage
    // need to correct here so that more partitions are loaded by default for broadcast spatial join
    IO.writeParquet(enrichedWithEvents.repartition(users.toInt),
                    DUMMY_DATA_LOCALITY_PATH_INPUT,
                    SaveMode.Overwrite)

    val enrichedWithEventsDisk = spark.read.parquet(DUMMY_DATA_LOCALITY_PATH_INPUT)
    IO.writeParquet(
      enrichedWithEventsDisk
        .transform(SqlTools.explodeDf(UserSpecificEvent.events))
        .transform(
          StructTypeHelpers.unpackSingleLevelStruct(UserSpecificEvent.events)),
      DUMMY_DATA_RAW_INPUT,
      SaveMode.Overwrite
    )
  }

  private def generateUserDays(users: Long,
                               yearMin: Int,
                               yearMax: Int,
                               periods: Int): Seq[(Long, Seq[Date])] = {
    val start = LocalDate.of(yearMin, Month.JANUARY, 1)
    val stop = LocalDate.of(yearMax, Month.JANUARY, 1)
    val maxDays = ChronoUnit.DAYS.between(start, stop)

    (1L to users).par
      .map(user => {
        (user,
         for (_ <- 1 until periods + 1)
           yield
             Date.valueOf(
               start.plusDays(
                 ThreadLocalRandom.current().nextLong(0, maxDays + 1))))
      })
      .seq
  }

  private def enrichEvents(c: PoiBenchmark)(df: DataFrame): DataFrame = {
    val boundaryBroadcast = df.sparkSession.sparkContext
      .broadcast(WKTUtils.read(c.boundingWKTGeometry))

    def generateEventsForDay(date: Date, baseNumberOfEvents: Int): Seq[Event] = {
      val localDate = date.toLocalDate
      val offset = Timestamp.valueOf(localDate.atStartOfDay()).getTime
      val end = Timestamp.valueOf(localDate.atTime(LocalTime.MAX)).getTime
      val diff = end - offset + 1
      val random = ThreadLocalRandom.current()
      val numberOfEvents =
        Math.abs(random.nextGaussian() * baseNumberOfEvents).toInt
      getKEventsWithinBoundary(numberOfEvents,
                               boundaryBroadcast.value,
                               random,
                               offset,
                               diff,
                               c.baseRadiusMin,
                               c.baseRadiusMax)
    }

    val eventsGenerationUDF = udf(generateEventsForDay _)

    df.withColumn(UserSpecificEvent.events,
                  eventsGenerationUDF(col(UserSpecificEvent.periods),
                                      lit(c.baseNumberOfEvents)))
  }

  private def getKEventsWithinBoundary(k: Int,
                                       boundary: Geometry,
                                       random: ThreadLocalRandom,
                                       offset: Long,
                                       diff: Long,
                                       baseRadiusMin: Double,
                                       baseRadiusMax: Double): Array[Event] = {
    var dumyPoints = new ArrayBuffer[Event](k)
    var fillStatus = 0
    while (fillStatus < k) {
      // draw a random point (x,y)
      // if it intersects with the boundary, keep it. NOTE: it could be faster if pre-limited to desired MBR
      // good enough for AT where both coordinates are positive
      // max values as defined in https://stackoverflow.com/questions/15965166/what-is-the-maximum-length-of-latitude-and-longitude
      val xLongCandidate = random.nextDouble(180)
      val yLatCandidate = random.nextDouble(90)
      val pointCandidate =
        geomFactory.createPoint(new Coordinate(xLongCandidate, yLatCandidate))
      if (pointCandidate.within(boundary)) {
        dumyPoints = dumyPoints :+ Event(
          new Timestamp(offset + (random.nextDouble * diff).toInt),
          xLongCandidate,
          yLatCandidate,
          DummyAreaGenerator.getDummyArea(pointCandidate,
                                          random,
                                          baseRadiusMin,
                                          baseRadiusMax)
        )
        fillStatus += 1
      }
    }
    dumyPoints.toArray.sortBy(_.event_time)
  }

}
