// Copyright (C) 2019 Georg Heiler
package at.csh.geoheil.common.transformer.geometry

import at.csh.geoheil.common.config.SpatialUtilsAll
import com.holdenkarau.spark.testing.{DatasetSuiteBase, SharedSparkContext}
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

class GeometryServiceTest
    extends FlatSpec
    with Matchers
    with SharedSparkContext
    with GivenWhenThen
    with DatasetSuiteBase {

  "GeometryService" should "perform a spatial join" in {
    import spark.implicits._
    val idColumn = "id"
    val geometryStringColumn = "geom"
    val geometryTypedColumn = "geom_geom"
    val areaOfInterest = "LINESTRING ( 0 0, 0 2)"
    val df = Seq((1, "POINT(0 0)"), (2, "POINT(40 17)"))
      .toDF(idColumn, geometryStringColumn)

    SpatialUtilsAll.registerSpatialFunctions(spark)
    Given("a DataFrame of geometries and a single scalar geometry")
    val result = df
      .transform(
        GeometryService.parseWktToGeomesaGeometry(geometryStringColumn,
                                                  geometryTypedColumn))
      .transform(
        GeometryService.filterSpaceScalar(geometryTypedColumn, areaOfInterest))
      .as[(Int, String)]
      .collect

    result.size should equal(1)
    result.head._1 should equal(1)

    Given(
      "a DataFrame of geometries and another DataFrame (vector) of geometries")
    val i2dColumn = "id"
    val g2eometryStringColumn = "geom"
    val g2eometryTypedColumnLeft = "geom_geom_left"
    val g2eometryTypedColumnRight = "geom_geom_right"
    val r2ight = Seq(
      (1,
       "POLYGON((5.615234375 32.959626461263795,35.41015625 32.959626461263795,35.41015625 14.994444201864106,5.615234375 14.994444201864106,5.615234375 32.959626461263795))"),
      (2,
       "POLYGON((4.912109375 20.73605317381076,9.5703125 29.325174414806934,17.919921875 31.822007220441396,25.830078125 25.978267331461943,4.912109375 20.73605317381076))")
    ).toDF(i2dColumn, g2eometryStringColumn)
    val left = Seq((1, "POINT(10.625 27.339360743998576)"),
                   (2, "POINT(29.08203125 20.434002156249797)"))
      .toDF(i2dColumn, g2eometryStringColumn)

    SpatialUtilsAll.registerSpatialFunctions(spark)
    // NOTE: we cannot use geomesa geometries for geoSpark operation.
    // Therefore geometries should always only temporarily be stored as objects.
    // Their plain basic types should be kept around for further operations(if desired)
    val l2eftGeom = left.transform(
      GeometryService.parseWktToGeoSparkGeometry(g2eometryStringColumn,
                                                 g2eometryTypedColumnLeft))
    val r2ightGeom = r2ight.transform(
      GeometryService.parseWktToGeoSparkGeometry(g2eometryStringColumn,
                                                 g2eometryTypedColumnRight))
    val result2 = GeometryService
      .filterSpaceVector(l2eftGeom,
                         g2eometryTypedColumnLeft,
                         r2ightGeom,
                         g2eometryTypedColumnRight)
    result2.count should equal(3)
  }
}
