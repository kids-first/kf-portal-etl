package io.kf.etl.repo

import java.net.URL

trait Repository {
  def getPrograms(): Seq[URL]
  def getProjectsByProgram(program_id:String): Seq[URL]
  def getFilesByProgramAndProject(program_id:String, project_id:String): Seq[URL]
}
