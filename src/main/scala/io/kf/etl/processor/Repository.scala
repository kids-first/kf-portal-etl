package io.kf.etl.processor

import java.net.URL


/**
  * Repository assumes the directory structure of the data storage is program/project/files
  */
trait Repository {
  def getPrograms(): Seq[(String, URL)]
  def getProjectsByProgram(program:URL): Seq[(String, URL)]
  def getFilesByProject(project: URL): Seq[(String, URL)]
}
