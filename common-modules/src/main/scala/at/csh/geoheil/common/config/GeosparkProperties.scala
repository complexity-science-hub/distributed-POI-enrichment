// Copyright (C) 2019 Georg Heiler
package at.csh.geoheil.common.config

import enumeratum._

sealed trait GeosparkSpatialPartitioningGridType extends EnumEntry

object GeosparkSpatialPartitioningGridType
    extends Enum[GeosparkSpatialPartitioningGridType] {
  val values = findValues

  case object quadtree extends GeosparkSpatialPartitioningGridType

  case object kdbtree extends GeosparkSpatialPartitioningGridType

  case object rtree extends GeosparkSpatialPartitioningGridType

  case object voronoi extends GeosparkSpatialPartitioningGridType

}

sealed trait GeosparkSpatialIndexType extends EnumEntry

object GeosparkSpatialIndexType extends Enum[GeosparkSpatialIndexType] {
  val values = findValues

  case object rtree extends GeosparkSpatialIndexType

  case object quadtree extends GeosparkSpatialIndexType

}
