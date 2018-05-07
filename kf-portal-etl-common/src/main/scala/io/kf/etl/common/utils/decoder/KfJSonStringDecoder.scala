package io.kf.etl.common.utils.decoder

import com.trueaccord.scalapb.GeneratedMessageCompanion

object KfJSonStringDecoder {

  private lazy val scalaPbJson4sParser = new com.trueaccord.scalapb.json.Parser(preservingProtoFieldNames = true)

  def parseJson[T <: com.trueaccord.scalapb.GeneratedMessage with com.trueaccord.scalapb.Message[T]](json:String)(implicit cmp: GeneratedMessageCompanion[T]): T = {
    scalaPbJson4sParser.fromJsonString[T](json)
  }
}
