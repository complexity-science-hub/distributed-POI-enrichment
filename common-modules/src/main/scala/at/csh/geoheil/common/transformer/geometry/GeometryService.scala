// Copyright (C) 2019 Georg Heiler
package at.csh.geoheil.common.transformer.geometry

import java.util.UUID.randomUUID

import at.csh.geoheil.common.SparkUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.types.DecimalType
import org.locationtech.geomesa.spark.jts.{st_geomFromWKT, st_point}
import org.slf4j.LoggerFactory

object GeometryService {

  @transient private lazy val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * Perform a spatial join of left and right geometries.    *
    *
    * @param leftLarge           geometries data frame (large)
    * @param leftGeometryCol     column containing the left geometries (already as parsed geometry type), Must not be Null
    * @param right               geometries data frame (small)
    * @param rightGeometryColumn column containing the right geometries (already as parsed geometry type), Must not be Null
    * @param spatialPredicate    spatial filter predicate, defaults to ST_intersects.
    * @return spatial INNER join of left and right
    */
  def filterSpaceVector(
      leftLarge: DataFrame,
      leftGeometryCol: String,
      right: DataFrame,
      rightGeometryColumn: String,
      spatialPredicate: STGeoSparkPredicate =
        STGeoSparkPredicate.geospark_ST_Intersects): DataFrame = {

    logger.info(
      s"Number of partitions: LEFT: ${leftLarge.rdd.getNumPartitions}, RIGHT: ${right.rdd.getNumPartitions}")
    // TODO remove println after logger config works. Currently, this is not displayed!
    println(
      s"Number of partitions: LEFT: ${leftLarge.rdd.getNumPartitions}, RIGHT: ${right.rdd.getNumPartitions}")
    // Make sure that we do not overwrite a tempory table in case a spatial filter is applied multiple times in dac
    // remove "-" because spark can't handle it in Strings
    val uuid = randomUUID().toString.replaceAll("[\\s\\-()]", "")
    val prefix = "tmp_"
    val leftTable = s"${prefix}left_$uuid"
    val rightTable = s"${prefix}right_$uuid"

    right.createOrReplaceTempView(rightTable)
    leftLarge.createOrReplaceTempView(leftTable)

    leftLarge.sparkSession
      .sql(s"""
              |SELECT *
              |FROM $leftTable JOIN $rightTable
              |  ON ${spatialPredicate.entryName}($leftTable.$leftGeometryCol,
              |    $rightTable.$rightGeometryColumn)
        """.stripMargin)
  }

  /**
    * Filter geometries by scalar area of interest
    *
    * @param leftLarge        geometries data frame
    * @param leftGeometryCol  column containing the geometry (already as geometry type), Must not be Null
    * @param areaOfInterest   WKT WGS84 linestring
    * @param spatialPredicate spatial filter predicate, defaults to ST_intersects.
    * @return leftLarge filtered to areaOfInterest by ST_I and drop geometry column
    */
  def filterSpaceScalar(leftGeometryCol: String,
                        areaOfInterest: String,
                        spatialPredicate: STGeomesaPredicate =
                          STGeomesaPredicate.ST_Intersects)(
      leftLarge: DataFrame): DataFrame = {
    leftLarge
      .filter(expr(
        s"${spatialPredicate.entryName}($leftGeometryCol, ${STGeomesaFunction.ST_geomFromWKT.entryName}('$areaOfInterest'))"))
      .drop(leftGeometryCol)
  }

  /**
    * Parse WKT string to geomesa geometry
    *
    * @param wktString            column which contains the WKT formatted linestring
    * @param resultGeometryColumn output column with geometry
    * @param df                   which contains original WKT formatted geometries as string
    * @return additional column with parsed geometry
    */
  def parseWktToGeomesaGeometry(
      wktString: String,
      resultGeometryColumn: String)(df: DataFrame): DataFrame = {
    df.withColumn(
      resultGeometryColumn,
      SparkUtils.nullableFunction(st_geomFromWKT(col(wktString)), wktString))
  }

  /**
    * Parse WKT string to geoSpark geometry
    *
    * @param wktString            column which contains the WKT formatted linestring
    * @param resultGeometryColumn output column with geometry
    * @param df                   which contains original WKT formatted geometries as string
    * @return additional column with parsed geometry
    */
  def parseWktToGeoSparkGeometry(
      wktString: String,
      resultGeometryColumn: String)(df: DataFrame): DataFrame = {
    df.withColumn(
      resultGeometryColumn,
      SparkUtils.nullableFunction(
        expr(
          s"${STGeoSparkFunction.geospark_ST_GeomFromWkt.entryName}($wktString)"),
        wktString))
  }

  /**
    * Parse x,y columns to geomesa geometry
    *
    * @param xLongColumn          x longitude column name
    * @param yLatColumn           y latitude column name
    * @param resultGeometryColumn name of resulting geometry column
    * @param df                   spark dataframe with plain geometries
    * @return df with parsed point to geomesa geometry
    */
  def parseXYToGeomesaPointGeometry(
      xLongColumn: String,
      yLatColumn: String,
      resultGeometryColumn: String)(df: DataFrame): DataFrame = {
    df.withColumn(
      resultGeometryColumn,
      SparkUtils.nullableFunction(st_point(col(xLongColumn), col(yLatColumn)),
                                  xLongColumn,
                                  yLatColumn))
  }

  /**
    * Parse x,y columns to geoSpark geometry
    *
    * @param xLongColumn          x longitude column name
    * @param yLatColumn           y latitude column name
    * @param resultGeometryColumn name of resulting geometry column
    * @param df                   spark dataframe with plain geometries
    * @return df with parsed point to geoSpark geometry
    */
  def parseXYToGeosparkPointGeometry(
      xLongColumn: String,
      yLatColumn: String,
      resultGeometryColumn: String)(df: DataFrame): DataFrame = {
    df.withColumn(xLongColumn, col(xLongColumn).cast(DecimalType(38, 18)))
      .withColumn(yLatColumn, col(yLatColumn).cast(DecimalType(38, 18)))
      .withColumn(
        resultGeometryColumn,
        SparkUtils.nullableFunction(
          expr(
            s"${STGeoSparkFunction.geospark_ST_Point.entryName}($xLongColumn, $yLatColumn)"),
          xLongColumn,
          yLatColumn)
      )
  }
}
