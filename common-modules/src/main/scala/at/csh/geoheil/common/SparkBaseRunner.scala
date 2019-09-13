// Copyright (C) 2019 Georg Heiler
package at.csh.geoheil.common

import at.csh.geoheil.common.config.ConfigurationUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import pureconfig.ConfigReader

abstract class SparkBaseRunner[T <: Product: ConfigReader](
    useKryo: Boolean = true)
    extends App {

  @transient lazy val logger = LoggerFactory.getLogger(this.getClass)

  val c = ConfigurationUtils.loadConfiguration[T]

  def createSparkConfiguration(appName: String): SparkConf =
    ConfigurationUtils.createConf(appName, useKryo)

  def createSparkSession(sparkConf: SparkConf): SparkSession =
    ConfigurationUtils.createSparkSession(sparkConf)
}
