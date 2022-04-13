package io.kf.etl.common

import com.typesafe.config.Config
import io.kf.etl.common.Constants._

object Utils {

  def calculateDataCategory(
                             availableDataType: Seq[String],
                             mapOfDataCategory_ExistingTypes: Map[String, Seq[String]]
                           ): Set[String] = {
    val cleanAvailableDataTypes = availableDataType.map(_.toLowerCase.trim)
    mapOfDataCategory_ExistingTypes.collect {
      case (dCategories, aTypes) if(cleanAvailableDataTypes.exists(t => aTypes.contains(t))) => dCategories
    }.toSet
  }

  def getOptionalConfig(value: String, conf: Config) = {
    try {
      Some(conf.getString(value))
    } catch {
      case e: Exception => None
    }
  }
}