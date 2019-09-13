// Copyright (C) 2019 Georg Heiler
package at.csh.geoheil.common.transformer

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DataType, StructType}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}

import scala.reflect.runtime.universe._

// https://github.com/MrPowers/spark-daria/blob/842cfe37e86fb896f4d0fcc685eed700915c9a73/src/main/scala/com/github/mrpowers/spark/daria/sql/types/StructTypeHelpers.scala
object StructTypeHelpers {

  def unpackSingleLevelStruct(structColumnName: String)(
      df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._
    val otherColumns = df.drop(structColumnName).columns
    df.select(
      (otherColumns
        .map(col) :+ $"${structColumnName}.*"): _*)
  }

  def flattenSchema(schema: StructType,
                    delimiter: String = "__",
                    prefix: String = null): Array[Column] = {
    var arrayCounter = 0
    schema.fields.flatMap(structField => {
      val codeColName =
        if (prefix == null) structField.name
        else prefix + "." + structField.name
      val colName =
        if (prefix == null) structField.name
        else prefix + delimiter + structField.name

      structField.dataType match {
        case st: StructType =>
          flattenSchema(
            schema = st,
            delimiter = delimiter,
            prefix = colName
          )
        case _: ArrayType => {
          if (arrayCounter == 0) {
            arrayCounter += 1
            Array(explode(col(colName)))
          } else {
            Array(col(colName))
          }
        }
        case _ => Array(col(codeColName).alias(colName))
      }
    })
  }

  def selectColumnsByType(schem: StructType,
                          columnDataType: DataType): Seq[String] = {
    schem.filter(_.dataType.equals(columnDataType)).map(_.name)
  }

  def createEmptyDataFrameForSchema(schema: StructType)(
      implicit spark: SparkSession): DataFrame = {
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
  }

  def schemaFromCaseClass[T <: Product: TypeTag]: StructType =
    ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType]

}

object DataFrameExt {

  implicit class DataFrameMethods(df: DataFrame) {

    def flattenSchema(delimiter: String = "__",
                      prefix: String = null): DataFrame = {
      df.select(
        StructTypeHelpers.flattenSchema(df.schema, delimiter, prefix): _*
      )
    }

    def selectColumnsByType(columnDataType: DataType): DataFrame = {
      df.select(
        StructTypeHelpers
          .selectColumnsByType(df.schema, columnDataType)
          .map(col): _*)
    }

  }

}

object SchemaTools {

  /**
    * Unifies the schema of dataFrames. All distinct  input column names are returned.
    * In case a column does not exist in the first place it is automatically created with content of null
    *
    * @param dfs list of non congruent (schema is assumed to be different) dataFrames
    * @return flattened schema
    */
  def unionCombineDfOfAlreadyUnifiedSchema(dfs: Seq[DataFrame]): DataFrame = {
    val allColumns = getAllColumns(dfs)
    dfs
      .map(currentDf => {
        // iterate over all columns if exists, do nothing, if missing add empty (null) column
        val mergedSchema = allColumns.foldLeft(currentDf) {
          (intermediate, column) =>
            {
              if (hasColumn(intermediate, column)) {
                intermediate
              } else {
                intermediate.withColumn(column, lit(null))
              }
            }
        }

        // select in order (force same columns are concatenated (UNION) later on
        mergedSchema.select(allColumns.map(col): _*)
      })
      .reduce(_ union _)
  }

  private def getAllColumns(dfs: Seq[DataFrame]): Seq[String] = {
    dfs.flatMap(_.columns).distinct
  }

  private def hasColumn(df: DataFrame, column: String) =
    df.columns.map(_.toLowerCase).contains(column.toLowerCase)
}
