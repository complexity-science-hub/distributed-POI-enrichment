// Copyright (C) 2019 Georg Heiler
package at.csh.geoheil.poi

import at.csh.geoheil.common.config.OSMRawConfiguration
import at.csh.geoheil.common.model.raw.OSMNode
import at.csh.geoheil.common.provider.OSMRawProvider
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object OSMParser {

  /**
    * Inclusive filters. All chained with OR. Speciality: when the value in tagFilters is set to `isNotNull` instead of
    * filtering to a specific value sparks `isNotNull` operation is applied.
    * Afterwards, separate columns are extracted for each unique key in the `tagFilters`.
    *
    * @param osmConfig         raw configuration to load OSM nodes
    * @param tagFilters        Map of key, value filters. If the value is `isNotNull` null values are discarded
    * @param additionalColumns additional columns to be extracted, key: column name, value: content to be parsed
    * @param spark             Sparksession as implicit parameter
    * @return only OSM Nodes filtered to desired tags
    */
  def loadOsmNodesAndFilterToDesiredKeys(
      osmConfig: OSMRawConfiguration,
      tagFilters: Map[String, Seq[String]],
      additionalColumns: Map[String, String])(
      implicit spark: SparkSession): DataFrame = {
    val (node) = OSMRawProvider.provide(osmConfig)

    val combinedAdditionalColumns = additionalColumns ++ tagFilters.map(e =>
      (e._1, e._1))

    node
      .filter(generateFilterCondition(tagFilters))
      .transform(addAdditionalColumns(combinedAdditionalColumns))
      .drop(OSMNode.timestamp, OSMNode.version)
      .drop(OSMNode.tags)
  }

  private def generateFilterCondition(
      tagFilters: Map[String, Seq[String]]): Column = {
    val filterColumns = for {
      filterRecord <- tagFilters
      filterElement <- filterRecord._2
    } yield
      if (filterElement == "isNotNull") {
        col(OSMNode.tags).getItem(filterRecord._1).isNotNull
      } else {
        col(OSMNode.tags).getItem(filterRecord._1) === filterElement
      }
    filterColumns.reduce(_ or _)
  }

  private def addAdditionalColumns(additionalColumns: Map[String, String])(
      df: DataFrame): DataFrame = {
    additionalColumns
      .foldLeft(df) {
        case (acc, (columnName, tagToParse)) =>
          acc.withColumn(columnName, col(OSMNode.tags).getItem(tagToParse))
      }
  }

}
