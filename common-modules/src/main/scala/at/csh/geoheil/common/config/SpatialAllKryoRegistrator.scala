// Copyright (C) 2019 Georg Heiler
package at.csh.geoheil.common.config

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.locationtech.geomesa.spark.GeoMesaSparkKryoRegistrator

class SpatialAllKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {

    val commonReg = new CommonKryoRegistrator
    val geomesaReg = new GeoMesaSparkKryoRegistrator
    val geosparkKryoReg = new GeoSparkKryoRegistrator

    commonReg.registerClasses(kryo)
    geomesaReg.registerClasses(kryo)
    geosparkKryoReg.registerClasses(kryo)

    // further required geo-spatial classes
    kryo.register(
      classOf[
        scala.Array[org.datasyslab.geospark.spatialRddTool.StatCalculator]])
    kryo.register(
      classOf[org.datasyslab.geospark.spatialRddTool.StatCalculator])
    kryo.register(
      classOf[org.locationtech.jts.geom.impl.CoordinateArraySequence])
    kryo.register(classOf[org.locationtech.jts.geom.Polygon])
    kryo.register(classOf[org.locationtech.jts.geom.Polygon])
    kryo.register(classOf[org.locationtech.jts.geom.Coordinate])
    kryo.register(classOf[Array[org.locationtech.jts.geom.Coordinate]])
    kryo.register(classOf[Array[org.locationtech.jts.geom.Polygon]])
    kryo.register(classOf[Array[org.locationtech.jts.geom.LinearRing]])
    kryo.register(classOf[org.locationtech.jts.geom.LinearRing])
    kryo.register(classOf[org.locationtech.jts.geom.GeometryFactory])
    kryo.register(classOf[org.locationtech.jts.geom.PrecisionModel])
    kryo.register(
      Class.forName("org.locationtech.jts.geom.PrecisionModel$Type"))
    kryo.register(
      classOf[org.locationtech.jts.geom.impl.CoordinateArraySequenceFactory])
    // kryo.register(classOf[])
    kryo.register(Class.forName("[[B"))

    ()
  }
}
