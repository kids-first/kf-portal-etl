package io.kf.etl.processors.index.mapping

import io.kf.etl.common.Constants._

import scala.io.Source
object MappingFiles {

  def getMapping(index_name:String):String = {
    index_name match {
      case FILE_CENTRIC_PROCESSOR_NAME => Source.fromInputStream(MappingFiles.getClass.getResourceAsStream(s"/${FILE_CENTRIC_MAPPING_FILE_NAME}")).mkString
      case PARTICIPANT_CENTRIC_PROCESSOR_NAME => Source.fromInputStream(MappingFiles.getClass.getResourceAsStream(s"/${PARTICIPANT_CENTRIC_MAPPING_FILE_NAME}")).mkString
    }
  }
}
