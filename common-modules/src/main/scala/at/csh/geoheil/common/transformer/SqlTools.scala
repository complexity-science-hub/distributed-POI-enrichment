// Copyright (C) 2019 Georg Heiler
package at.csh.geoheil.common.transformer

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode, udf}
import org.locationtech.geomesa.spark.GeometricDistanceFunctions
import org.locationtech.jts.geom.Geometry

object SqlTools {

  def explodeDf(colToExplode: String)(dfInput: DataFrame): DataFrame = {
    val columns = dfInput.drop(colToExplode).columns.map(col)
    dfInput.select(
      columns :+ explode(col(colToExplode)).alias(colToExplode): _*)
  }

  /**
    * Geomesa does not register it  by default. So here, it is manually made available
    */
  val geomesa_ST_DistanceSpheroid = udf((g1: Geometry, g2: Geometry) =>
    GeometricDistanceFunctions.ST_DistanceSpheroid(g1, g2))

}
