// Copyright (C) 2019 Georg Heiler
package at.csh.geoheil.common.config

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.locationtech.geomesa.spark.GeometricDistanceFunctions
import org.locationtech.geomesa.spark.jts._

object SpatialUtilsAll {

  def useSpatialSerializers(
      sc: SparkConf,
      geosparkJoinGridType: GeosparkSpatialPartitioningGridType =
        GeosparkSpatialPartitioningGridType.kdbtree,
      geosparkIndexType: GeosparkSpatialIndexType =
        GeosparkSpatialIndexType.quadtree,
      geosparkJoinPartitions: Int = -1): SparkConf =
    sc.set("spark.kryo.registrator", classOf[SpatialAllKryoRegistrator].getName)
      .set("geospark.join.gridtype", geosparkJoinGridType.toString)
      .set("geospark.global.indextype", geosparkIndexType.toString)
      .set("geospark.join.numpartitions", geosparkJoinPartitions.toString)

  def registerSpatialFunctions(spark: SparkSession): Unit = {
    CustomGeosparkRegistrator.registerAll(spark)
    spark.withJTS
    GeometricDistanceFunctions.registerFunctions(spark.sqlContext)
    ()
  }
}
