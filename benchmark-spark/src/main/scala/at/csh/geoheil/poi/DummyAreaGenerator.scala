// Copyright (C) 2019 Georg Heiler
package at.csh.geoheil.poi

import java.util.concurrent.ThreadLocalRandom

import org.locationtech.geomesa.spark.jts.udf.GeometricProcessingFunctions.ST_antimeridianSafeGeom
import org.locationtech.geomesa.spark.jts.util.WKTUtils
import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory, Point}
import org.locationtech.jts.util.GeometricShapeFactory
import org.locationtech.spatial4j.context.jts.JtsSpatialContext
import org.locationtech.spatial4j.distance.DistanceUtils
import org.locationtech.spatial4j.shape.Circle
import org.locationtech.spatial4j.shape.jts.JtsPoint

/**
  * Based on Geomesa's ST_BufferPoint function from org.locationtech.geomesa.spark.jts.udf
  */
object DummyAreaGenerator {

  @transient private lazy val spatialContext = JtsSpatialContext.GEO
  @transient private lazy val geometryFactory = new GeometryFactory()
  @transient private val geometricShapeFactory =
    new ThreadLocal[GeometricShapeFactory] {
      override def initialValue(): GeometricShapeFactory = {
        new GeometricShapeFactory(geometryFactory)
      }
    }

  def getDummyArea(p: Point,
                   random: ThreadLocalRandom,
                   baseRadiusMin: Double,
                   baseRadiusMax: Double): String = {
    val radius = random.nextDouble(baseRadiusMin, baseRadiusMax)
    val degrees = DistanceUtils.dist2Degrees(radius / 1000.0,
                                             DistanceUtils.EARTH_MEAN_RADIUS_KM)
    val geom = fastCircleToGeom(
      new JtsPoint(p, spatialContext).getBuffered(degrees, spatialContext))
    WKTUtils.write(geom)
  }

  private def fastCircleToGeom(circle: Circle): Geometry = {
    val gsf = geometricShapeFactory.get()
    gsf.setSize(circle.getBoundingBox.getWidth)
    gsf.setNumPoints(4 * 25) //multiple of 4 is best
    gsf.setCentre(new Coordinate(circle.getCenter.getX, circle.getCenter.getY))
    ST_antimeridianSafeGeom(gsf.createCircle())
  }

}
