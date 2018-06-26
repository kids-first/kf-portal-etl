package io.kf.etl.test.common

import java.net.URL

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import io.kf.etl.common.url.KfURLStreamHandlerFactory
import org.scalatest._

import scala.io.Source

abstract class KfEtlUnitTestSpec extends FlatSpec with Matchers with
  OptionValues with Inside with Inspectors {

  val profile = Source.fromInputStream(classOf[KfEtlUnitTestSpec].getResourceAsStream("/aws_profile.properties")).mkString

  URL.setURLStreamHandlerFactory(new KfURLStreamHandlerFactory(
    AmazonS3ClientBuilder.standard().withCredentials(new ProfileCredentialsProvider(profile)).build()
  ))
}
