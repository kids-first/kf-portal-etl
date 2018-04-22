package io.kf.etl.processors.download

object ScalaPBJson4sMain extends App {

  val parser = new com.trueaccord.scalapb.json.Parser(preservingProtoFieldNames = true)

  parser.fromJsonString("")

}
