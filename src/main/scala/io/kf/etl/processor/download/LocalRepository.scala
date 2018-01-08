package io.kf.etl.processor.download

import java.io.File
import java.net.URL

import io.kf.etl.conf.RepositoryConfig
import io.kf.etl.processor.Repository

case class LocalRepository(private val repoConfig:RepositoryConfig, private val subDir:String) extends Repository{
  override def getPrograms(): Seq[(String, URL)] = {
    extract(s"${repoConfig.path}/${subDir}", false)
  }

  override def getProjectsByProgram(program: URL): Seq[(String, URL)] = {
    extract(program.getFile, false)
  }

  override def getFilesByProject(project: URL): Seq[(String, URL)] = {
    extract(project.getFile, true)
  }

  private def extract(path: String, checkingFile:Boolean):Seq[(String, URL)] = {
    val root = new File(path)
    List(root.listFiles().toSeq:_*).filter(file => (checkingFile && file.isFile) || (!checkingFile && file.isDirectory)).map(dir => (dir.getName, dir.toURI.toURL) )
  }
}
