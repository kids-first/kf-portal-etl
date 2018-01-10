package io.kf.etl.processor.download

import java.net.URL

import com.google.inject.Inject
import com.google.inject.name.Named
import io.kf.etl.conf.{HDFSConfig, RepositoryConfig}
import io.kf.etl.processor.Repository
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.mutable.ListBuffer

case class HDFSRepository(private val fs:FileSystem, private val repoConfig:RepositoryConfig, private val subPath:String) extends Repository{


  override def getPrograms(): List[(String, URL)] = {
    extract(new URL(s"${repoConfig.path}/${subPath}"), false)
  }

  override def getProjectsByProgram(program: URL): List[(String, URL)] = {
    extract(program, false)
  }

  override def getFilesByProject(project: URL): List[(String, URL)] = {
    extract(project, true)
  }

  private def extract(url: URL, checkingFile: Boolean): List[(String, URL)] = {
    val rets = new ListBuffer[(String, URL)]
    val remoteIterator = fs.listFiles(new Path(url.getPath), false)
    while(remoteIterator.hasNext) {
      val remote = remoteIterator.next()
      if((checkingFile && remote.isFile) || (!checkingFile && remote.isDirectory)) {
        rets += ((remote.getPath.getName, remote.getPath.toUri.toURL))
      }
    }
    rets.toList
  }


}
