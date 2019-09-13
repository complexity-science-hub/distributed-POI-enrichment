// Copyright (C) 2019 Georg Heiler
package at.csh.geoheil.poi

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.Row

case class Event(event_time: Timestamp,
                 x_long: Double,
                 y_lat: Double,
                 area: String)

case object Event {
  val area = "area"
  val x_long = "x_long"
  val y_lat = "y_lat"
  val event_time = "event_time"

  def fromRow(row: Row): Event =
    Event(
      event_time = row.getAs[Timestamp](Event.event_time),
      x_long = row.getAs[Double](Event.x_long),
      y_lat = row.getAs[Double](Event.y_lat),
      area = row.getAs[String](Event.area)
    )
}

case class UserSpecificEvent(id: Long, periods: Date, events: Seq[Event])

case object UserSpecificEvent {
  val id = "ide"
  val periods = "periods"
  val events = "events"
}

case class TimingResult(key: String, timing: Double)
case class TimingResultWithLoad(load: Long, results: TimingResult)
