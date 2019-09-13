// Copyright (C) 2019 Georg Heiler
package at.csh.geoheil.common.config

final case class ConfigurationException(message: String)
    extends Exception(message)
