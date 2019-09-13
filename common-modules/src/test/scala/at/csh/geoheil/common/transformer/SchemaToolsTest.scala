// Copyright (C) 2019 Georg Heiler
package at.csh.geoheil.common.transformer

import com.holdenkarau.spark.testing.{DatasetSuiteBase, SharedSparkContext}
import org.scalatest.{FlatSpec, Matchers}

class SchemaToolsTest
    extends FlatSpec
    with Matchers
    with SharedSparkContext
    with DatasetSuiteBase {

  "SchemaTools" should "combine dataFrames with same schema" in {
    import spark.implicits._

    val df1 = Seq((1, "a")).toDF("col1", "col2")
    val df2 = Seq((2, "b")).toDF("col1", "col2")

    val result =
      SchemaTools.unionCombineDfOfAlreadyUnifiedSchema(Seq(df1, df2))

    result.count.toInt should equal(2)
    result.columns should contain allOf ("col1", "col2")
    result.columns.length should equal(2)
  }

  it should "union dataFrames of different schemata" in {
    import spark.implicits._
    val df1 = Seq((1, "a")).toDF("col1", "col2")
    val df2 = Seq((2, "b", "c")).toDF("col1", "col2", "extra1")
    val df3 = Seq((2, "b", "d", "e")).toDF("col1", "col2", "extra1", "2_extra")
    val df4 = Seq((2, "b", "d", 44)).toDF("col1", "col2", "extra1", "44444")

    val result =
      SchemaTools.unionCombineDfOfAlreadyUnifiedSchema(Seq(df1, df2, df3, df4))

    result.count.toInt should equal(4)
    result.columns should contain allOf ("col1", "col2", "extra1", "44444", "2_extra")
    result.columns.length should equal(5)
  }
}
