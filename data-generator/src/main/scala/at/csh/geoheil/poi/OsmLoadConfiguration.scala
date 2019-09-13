// Copyright (C) 2019 Georg Heiler
package at.csh.geoheil.poi

import at.csh.geoheil.common.config.BaseConfiguration
import org.apache.spark.sql.SaveMode

case class OsmPaths(nodePath: String,
                    wayPath: String,
                    relationPath: String,
                    sharedPrefix: String,
                    saveMode: SaveMode,
                    outputFolder: String)
case class OsmLoadConfiguration(override val applicationName: String,
                                osmLoad: OsmPaths)
    extends BaseConfiguration
