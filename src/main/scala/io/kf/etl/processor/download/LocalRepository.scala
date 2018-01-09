package io.kf.etl.processor.download

import java.io.File
import java.net.URL

import io.kf.etl.conf.RepositoryConfig
import io.kf.etl.processor.Repository

case class LocalRepository(private val repoConfig:RepositoryConfig, private val subPath:String) extends Repository{
  override def getPrograms(): List[(String, URL)] = {
    extract(s"${repoConfig.path}/${subPath}", false)
  }

  override def getProjectsByProgram(program: URL): List[(String, URL)] = {
    extract(program.getFile, false)
  }

  override def getFilesByProject(project: URL): List[(String, URL)] = {
    extract(project.getFile, true)
  }

  private def extract(path: String, checkingFile:Boolean):List[(String, URL)] = {
    val root = new File(path)
    List(root.listFiles().toSeq:_*).filter(file => (checkingFile && file.isFile) || (!checkingFile && file.isDirectory)).map(dir => (dir.getName, dir.toURI.toURL) )
  }
}
