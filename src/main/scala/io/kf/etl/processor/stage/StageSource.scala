package io.kf.etl.processor.stage

import io.kf.etl.conf.RepositoryConfig
import io.kf.etl.processor.Repository
import io.kf.etl.processor.download.LocalRepository

class StageSource(private val repoConfig:RepositoryConfig, private val subPath:String) {

  def getRepository(): Repository = {
    new LocalRepository(repoConfig, subPath)
  }
}
