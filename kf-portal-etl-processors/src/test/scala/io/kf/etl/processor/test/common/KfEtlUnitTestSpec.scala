package io.kf.etl.processor.test.common

import org.scalatest._

abstract class KfEtlUnitTestSpec extends FlatSpec with Matchers with
  OptionValues with Inside with Inspectors
