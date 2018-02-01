package io.kf.etl.test.common

import org.scalatest._

abstract class KfUnitTestSpec extends FlatSpec with Matchers with
  OptionValues with Inside with Inspectors
