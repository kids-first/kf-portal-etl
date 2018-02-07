package io.kf.etl.common.context

import com.typesafe.config.Config
import io.kf.etl.common.conf.KFConfig

trait ContextTrait {
  lazy val config:KFConfig = loadConfig()

  def loadConfig(): KFConfig

  def getProcessConfig(name: String): Option[Config] = {
    config.processorsConfig.get(name)
  }}
