package io.kf.etl.common

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

}