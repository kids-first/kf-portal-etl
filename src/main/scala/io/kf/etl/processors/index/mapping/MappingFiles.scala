package io.kf.etl.processors.index.mapping

import scala.io.Source

object MappingFiles {

  def getMapping(index_name: String): String = {
    Source.fromInputStream(MappingFiles.getClass.getResourceAsStream(s"/resources/$index_name.mapping.json")).mkString
  }
}
