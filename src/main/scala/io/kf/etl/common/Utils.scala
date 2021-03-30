package io.kf.etl.common

object Utils {

    def calculateDataCategory(
                               availableDataType: Seq[String],
                               mapOfDataCategory_ExistingTypes: Map[String, Seq[String]]
                             ): Set[String] = {
      val cleanAvailableDataTypes = availableDataType.map(_.toLowerCase.trim)
      mapOfDataCategory_ExistingTypes.collect { case t if isIncludedInAvailableDt(t._2, cleanAvailableDataTypes) => t }.keySet
    }

  def isIncludedInAvailableDt(dataCategory: Seq[String], availableDataType: Seq[String]): Boolean = {
    dataCategory.collectFirst({case d if availableDataType.contains(d) => d}) } match {
    case Some(_) => true
    case _ => false
  }

}
