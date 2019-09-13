// Copyright (C) 2019 Georg Heiler
package at.csh.geoheil.common.transformer.cleanup

import org.apache.spark.sql.DataFrame

object Renamer {

  def renameDFtoLowerCase()(df: DataFrame): DataFrame =
    df.toDF(df.columns.map(_.toLowerCase): _*)

}
