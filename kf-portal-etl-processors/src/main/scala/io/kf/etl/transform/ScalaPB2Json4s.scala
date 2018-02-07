package io.kf.etl.transform

import java.util.Base64

import com.trueaccord.scalapb.GeneratedMessageCompanion
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods._

import scalapb.descriptors._

object ScalaPB2Json4s {

  implicit class PValue2JValue[T <: com.trueaccord.scalapb.GeneratedMessage with com.trueaccord.scalapb.Message[T]](entity: T)(implicit meta: GeneratedMessageCompanion[T]){
    // in scalapb_json4s, if the message field name contains underscore(_), all of the underscores are missing from the generated case classes, that means if the case classes are written into ES, the field name doesn't contain underscore
    // but in scalaDescriptor, the original field name is saved.
    // so here we build the custom method to use the saved meta-info in scalaDescriptor to generate json string
    def toJValue(): JValue = {


      def _PValueToJValue(pValue:PValue): JValue = {
        pValue match {
          case PEmpty => JNothing
          case PInt(value) => JInt(value)
          case PLong(value) => JInt(value)
          case PString(value) => JString(value)
          case PDouble(value) => JDouble(value)
          case PFloat(value) => JDouble(value)
          case PBoolean(value) => JBool(value)
          case PByteString(value) => {
            JString(Base64.getEncoder.encode(value.toByteArray).toString)
          }
          case PEnum(value) => JString(value.toString())
          case PMessage(map) => {
            JObject(
              map.map(entry => {
                (entry._1.name, _PValueToJValue(entry._2))
              }).toSeq:_*
            )
          }
          case PRepeated(vector) => {
            JArray(
              vector.map(_PValueToJValue(_)).toList
            )
          }
        }
      }

      _PValueToJValue(entity.toPMessage)

    }

    def toJsonString():String = {
      compact(render(toJValue()))
    }
  }

}
