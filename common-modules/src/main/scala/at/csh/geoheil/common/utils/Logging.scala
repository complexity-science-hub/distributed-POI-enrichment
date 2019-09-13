// Copyright (C) 2019 Georg Heiler
package at.csh.geoheil.common.utils

import org.slf4j.LoggerFactory

trait Logging {
  @transient lazy val logger = LoggerFactory.getLogger(this.getClass)
}
