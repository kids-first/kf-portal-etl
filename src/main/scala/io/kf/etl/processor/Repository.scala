package io.kf.etl.processor

import java.net.URL


/**
  * Repository assumes the directory structure of the data storage is program/project/files
  */
trait Repository {
  def getPrograms(): List[(String, URL)]
  def getProjectsByProgram(program:URL): List[(String, URL)]
  def getFilesByProject(project: URL): List[(String, URL)]
}
