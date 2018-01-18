package io.kf.etl.datasource

case class KidsFirstRelationProviderParametersMissingException(val keys: Set[String]) extends Exception("The must-have options are missing: " + keys)
