package io.kf.etl.repo.impl

import java.net.URL

import com.google.inject.Inject
import io.kf.etl.conf.RepoConfig
import io.kf.etl.repo.Repository

class LocalHttpServerRepository @Inject() (private val config: RepoConfig) extends Repository{
  override def getPrograms(): Seq[URL] = ???

  override def getProjectsByProgram(program_id: String): Seq[URL] = ???

  override def getFilesByProgramAndProject(program_id: String, project_id: String): Seq[URL] = {
    Seq(
      new URL(config.url + "/aliquot.tsv"),
      new URL(config.url + "/case.tsv"),
      new URL(config.url + "/demographic.tsv"),
      new URL(config.url + "/diagnosis.tsv"),
      new URL(config.url + "/read_group.tsv"),
      new URL(config.url + "/sample.tsv"),
      new URL(config.url + "/submitted_aligned_reads.tsv"),
      new URL(config.url + "/trio.tsv")
    )
  }
}
