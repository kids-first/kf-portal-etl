package io.kf.etl.processors.download.transform.utils

import com.typesafe.config.Config
import io.kf.etl.common.Constants.{CONFIG_NAME_DATASERVICE_DOMAIN_DCF, CONFIG_NAME_DATASERVICE_DOMAIN_GEN3, CONFIG_NAME_DATASERVICE_LIMIT, CONFIG_NAME_DATASERVICE_URL}

import scala.util.{Failure, Success, Try}

case class DataServiceConfig(url: String, limit: Int, gen3Host: String, dcfHost: String)

object DataServiceConfig {
  def apply(config: Config): DataServiceConfig = {
    DataServiceConfig(
      url = config.getString(CONFIG_NAME_DATASERVICE_URL),
      limit = Try(config.getInt(CONFIG_NAME_DATASERVICE_LIMIT)) match {
        case Success(value) => value
        case Failure(_) => 100
      },
      dcfHost = config.getString(CONFIG_NAME_DATASERVICE_DOMAIN_DCF),
      gen3Host = config.getString(CONFIG_NAME_DATASERVICE_DOMAIN_GEN3)
    )
  }
}