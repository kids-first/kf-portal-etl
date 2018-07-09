package io.kf.etl.processors.index.mapping

import io.kf.etl.common.Constants._

import scala.io.Source
object MappingFiles {

  def getMapping(index_name:String):String = {

    val pattern_file_centric = s"${FILE_CENTRIC_PROCESSOR_NAME}_(.+)".r
    val pattern_participant_centric = s"${PARTICIPANT_CENTRIC_PROCESSOR_NAME}_(.+)".r

    index_name match {
      case pattern_file_centric(id) => Source.fromInputStream(MappingFiles.getClass.getResourceAsStream(s"/${FILE_CENTRIC_MAPPING_FILE_NAME}")).mkString
      case pattern_participant_centric(id) => Source.fromInputStream(MappingFiles.getClass.getResourceAsStream(s"/${PARTICIPANT_CENTRIC_MAPPING_FILE_NAME}")).mkString
    }
  }
}
