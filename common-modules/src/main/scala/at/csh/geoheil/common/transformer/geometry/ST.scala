// Copyright (C) 2019 Georg Heiler
package at.csh.geoheil.common.transformer.geometry

import enumeratum._

import scala.collection.immutable

/**
  * Geospark does not offer proper UDF https://github.com/DataSystemsLab/GeoSpark/issues/252 as a quick workaround
  * i.e., instead of specifying strings specifying a refactorable object with IDE support geospark's functions are
  * listed here.
  */
sealed trait STGeoSparkPredicate extends EnumEntry

object STGeoSparkPredicate extends Enum[STGeoSparkPredicate] {

  val values: immutable.IndexedSeq[STGeoSparkPredicate] = findValues

  case object geospark_ST_Contains extends STGeoSparkPredicate

  case object geospark_ST_Intersects extends STGeoSparkPredicate

  case object geospark_ST_Within extends STGeoSparkPredicate

}

sealed trait STGeoSparkFunction extends EnumEntry

object STGeoSparkFunction extends Enum[STGeoSparkFunction] {

  val values: immutable.IndexedSeq[STGeoSparkFunction] = findValues

  case object geospark_ST_Area extends STGeoSparkFunction

  case object geospark_ST_Centroid extends STGeoSparkFunction

  case object geospark_ST_Circle extends STGeoSparkFunction

  case object geospark_ST_Convexhull extends STGeoSparkFunction

  case object geospark_ST_Distance extends STGeoSparkFunction

  case object geospark_ST_Envelope extends STGeoSparkFunction

  case object geospark_ST_EnvelopeAggr extends STGeoSparkFunction

  case object geospark_ST_GeomFromGeoJson extends STGeoSparkFunction

  case object geospark_ST_GeomFromWkb extends STGeoSparkFunction

  case object geospark_ST_GeomFromWkt extends STGeoSparkFunction

  case object geospark_ST_Intersection extends STGeoSparkFunction

  case object geospark_ST_Length extends STGeoSparkFunction

  case object geospark_ST_LinestringFromText extends STGeoSparkFunction

  case object geospark_ST_Point extends STGeoSparkFunction

  case object geospark_ST_PointFromText extends STGeoSparkFunction

  case object geospark_ST_PolygonFromEnvelope extends STGeoSparkFunction

  case object geospark_ST_PolygonFromText extends STGeoSparkFunction

  case object geospark_ST_Transform extends STGeoSparkFunction

  case object geospark_ST_UnionAggr extends STGeoSparkFunction

}

/**
  * List of typesafely available functions from https://www.geomesa.org/documentation/user/spark/sparksql_functions.html as string.
  * Usually, it is better to directly use the GEOMESA UDF. But sometimes using this string based methodology makes more sense
  */
sealed trait STGeomesaPredicate extends EnumEntry

object STGeomesaPredicate extends Enum[STGeomesaPredicate] {

  val values: immutable.IndexedSeq[STGeomesaPredicate] = findValues

  case object ST_Contains extends STGeomesaPredicate

  case object ST_Covers extends STGeomesaPredicate

  case object ST_Crosses extends STGeomesaPredicate

  case object ST_Disjoint extends STGeomesaPredicate

  case object ST_Equals extends STGeomesaPredicate

  case object ST_Intersects extends STGeomesaPredicate

  case object ST_Overlaps extends STGeomesaPredicate

  case object ST_Touches extends STGeomesaPredicate

  case object ST_Within extends STGeomesaPredicate

  case object ST_Relate extends STGeomesaPredicate

}

sealed trait STGeomesaFunction extends EnumEntry

object STGeomesaFunction extends Enum[STGeomesaFunction] {

  val values: immutable.IndexedSeq[STGeomesaFunction] = findValues

  case object ST_geomFromWKT extends STGeomesaFunction

}
