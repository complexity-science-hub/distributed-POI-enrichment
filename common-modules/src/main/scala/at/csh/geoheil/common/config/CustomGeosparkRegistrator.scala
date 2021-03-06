// Copyright (C) 2019 Georg Heiler
package at.csh.geoheil.common.config

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.geosparksql.UDT.UdtRegistratorWrapper
import org.apache.spark.sql.geosparksql.strategy.join.JoinQueryDetector
import org.datasyslab.geosparksql.UDF.Catalog

object CustomGeosparkRegistrator {

  @transient val geosparkPrefix = "geospark_"

  def registerAll(sparkSession: SparkSession): Unit = {
    sparkSession.experimental.extraStrategies = JoinQueryDetector :: Nil
    UdtRegistratorWrapper.registerAll()
    Catalog.expressions.foreach(
      f =>
        sparkSession.sessionState.functionRegistry.registerFunction(
          geosparkPrefix + f.getClass.getSimpleName.dropRight(1),
          f))
    Catalog.aggregateExpressions.foreach(f =>
      sparkSession.udf.register(geosparkPrefix + f.getClass.getSimpleName, f))
  }

}
