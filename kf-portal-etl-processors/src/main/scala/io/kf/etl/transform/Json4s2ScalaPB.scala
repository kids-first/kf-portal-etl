package io.kf.etl.transform

import com.fasterxml.jackson.core.Base64Variants
import com.google.protobuf.ByteString
import com.google.protobuf.descriptor.FieldDescriptorProto
import com.trueaccord.scalapb.GeneratedMessageCompanion
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods
import scalapb.descriptors._

object Json4s2ScalaPB {

  case class JsonFormatException(msg: String, cause: Exception) extends Exception(msg, cause) {
    def this(msg: String) = this(msg, null)
  }

  implicit class JValue2PValue[T <: com.trueaccord.scalapb.GeneratedMessage with com.trueaccord.scalapb.Message[T]](json:String)(implicit meta: GeneratedMessageCompanion[T]){

    def toScalaPB():T = {

      def _JValue2PMessage(jValue: JValue):PMessage = {
        jValue match {
          case JObject(values) => {
            val fields = values.map(k => k._1 -> k._2).toMap
            PMessage(
              (
                for{
                  fd <- meta.scalaDescriptor.fields
                  jsValue <- fields.get(fd.asProto.getName) if jsValue != JNull
                } yield (fd, parseJValue(fd, jsValue))
              ).toMap
            )
          }
          case _ => throw new JsonFormatException(s"Expected an object, found ${jValue}")
        }
      }

      def parseJValue(fd: FieldDescriptor, jValue: JValue):PValue = {


        if(fd.isMapField){
          jValue match {
            case JObject(vals) =>
              val mapEntryDesc = fd.scalaType.asInstanceOf[ScalaType.Message].descriptor
              val keyDescriptor = mapEntryDesc.findFieldByNumber(1).get
              val valueDescriptor = mapEntryDesc.findFieldByNumber(2).get
              PRepeated(vals.map {
                case (key, vv) =>
                  val keyObj = keyDescriptor.scalaType match {
                    case ScalaType.Boolean => PBoolean(java.lang.Boolean.valueOf(key))
                    case ScalaType.Double => PDouble(java.lang.Double.valueOf(key))
                    case ScalaType.Float => PFloat(java.lang.Float.valueOf(key))
                    case ScalaType.Int => PInt(java.lang.Integer.valueOf(key))
                    case ScalaType.Long => PLong(java.lang.Long.valueOf(key))
                    case ScalaType.String => PString(key)
                    case _ => throw new RuntimeException(s"Unsupported type for key for ${fd.name}")
                  }
                  PMessage(
                    Map(keyDescriptor -> keyObj,
                      valueDescriptor -> parseSingleValue(valueDescriptor, vv)))
              }(scala.collection.breakOut))
            case _ => throw new JsonFormatException(
              s"Expected an object for map field ${fd.asProto.getName} of ${fd.containingMessage.name}")
          }
        }
        else if(fd.isRepeated) {
          jValue match {
            case JArray(values) => PRepeated(values.map(parseSingleValue( fd, _)).toVector)
            case _ => throw new JsonFormatException(s"Expected an object for map field ${fd.asProto.getName} of ${fd.containingMessage.name}")
          }
        }
        else{
          parseSingleValue(fd, jValue)
        }
      }

      def parseSingleValue(fd:FieldDescriptor, jValue: JValue): PValue = {

        parsePrimitive(fd.scalaType, fd.protoType, jValue,
          throw new JsonFormatException(
            s"Unexpected value ($jValue) for field ${fd.asProto.getName} of ${fd.containingMessage.name}")
        )
      }

      meta.messageReads.read(_JValue2PMessage(toJValue()))

    }

    private def toJValue():JValue = {
      JsonMethods.parse(json)
    }


    private def parsePrimitive(scalaType: ScalaType, protoType: FieldDescriptorProto.Type, value: JValue, onError: => PValue): PValue = (scalaType, value) match {
      case (ScalaType.Int, JInt(x)) => PInt(x.intValue)
      case (ScalaType.Int, JDouble(x)) => PInt(x.intValue)
      case (ScalaType.Int, JDecimal(x)) => PInt(x.intValue)
      case (ScalaType.Int, JString(x)) if protoType.isTypeInt32 => parseInt32(x)
      case (ScalaType.Int, JString(x)) if protoType.isTypeSint32 => parseInt32(x)
      case (ScalaType.Int, JString(x)) => parseUint32(x)
      case (ScalaType.Long, JDecimal(x)) => PLong(x.longValue())
      case (ScalaType.Long, JString(x)) if protoType.isTypeInt64 => parseInt64(x)
      case (ScalaType.Long, JString(x)) if protoType.isTypeSint64 => parseInt64(x)
      case (ScalaType.Long, JString(x)) => parseUint64(x)
      case (ScalaType.Long, JInt(x)) => PLong(x.toLong)
      case (ScalaType.Double, JDouble(x)) => PDouble(x)
      case (ScalaType.Double, JInt(x)) => PDouble(x.toDouble)
      case (ScalaType.Double, JDecimal(x)) => PDouble(x.toDouble)
      case (ScalaType.Double, JString("NaN")) => PDouble(Double.NaN)
      case (ScalaType.Double, JString("Infinity")) => PDouble(Double.PositiveInfinity)
      case (ScalaType.Double, JString("-Infinity")) => PDouble(Double.NegativeInfinity)
      case (ScalaType.Float, JDouble(x)) => PFloat(x.toFloat)
      case (ScalaType.Float, JInt(x)) => PFloat(x.toFloat)
      case (ScalaType.Float, JDecimal(x)) => PFloat(x.toFloat)
      case (ScalaType.Float, JString("NaN")) => PFloat(Float.NaN)
      case (ScalaType.Float, JString("Infinity")) => PFloat(Float.PositiveInfinity)
      case (ScalaType.Float, JString("-Infinity")) => PFloat(Float.NegativeInfinity)
      case (ScalaType.Boolean, JBool(b)) => PBoolean(b)
      case (ScalaType.String, JString(s)) => PString(s)
      case (ScalaType.ByteString, JString(s)) =>
        PByteString(ByteString.copyFrom(Base64Variants.getDefaultVariant.decode(s)))
      case _ => onError
    }
    def parseBigDecimal(value: String): BigDecimal = {
      try {
        // JSON doesn't distinguish between integer values and floating point values so "1" and
        // "1.000" are treated as equal in JSON. For this reason we accept floating point values for
        // integer fields as well as long as it actually is an integer (i.e., round(value) == value).
        BigDecimal(value)
      } catch { case e: Exception =>
        throw JsonFormatException(s"Not a numeric value: $value", e)
      }
    }

    def parseInt32(value: String): PValue = {
      try {
        PInt(value.toInt)
      } catch { case _: Exception =>
        try {
          PInt(parseBigDecimal(value).toIntExact)
        } catch { case e: Exception =>
          throw JsonFormatException(s"Not an int32 value: $value", e)
        }
      }
    }

    def parseInt64(value: String): PValue = {
      try {
        PLong(value.toLong)
      } catch { case _: Exception =>
        val bd = parseBigDecimal(value)
        try {
          PLong(bd.toLongExact)
        } catch { case e: Exception =>
          throw JsonFormatException(s"Not an int64 value: $value", e)
        }
      }
    }

    def parseUint32(value: String): PValue = {
      try {
        val result = value.toLong
        if (result < 0 || result > 0xFFFFFFFFl) throw new JsonFormatException(s"Out of range uint32 value: $value")
        return PInt(result.toInt)
      } catch {
        case e: JsonFormatException => throw e
        case e: Exception => // Fall through.
      }
      parseBigDecimal(value).toBigIntExact().map { intVal =>
        if (intVal < 0 || intVal > 0xFFFFFFFFl) throw new JsonFormatException(s"Out of range uint32 value: $value")
        PLong(intVal.intValue())
      } getOrElse {
        throw new JsonFormatException(s"Not an uint32 value: $value")
      }
    }

    val MAX_UINT64 = BigInt("FFFFFFFFFFFFFFFF", 16)

    def parseUint64(value: String): PValue = {
      parseBigDecimal(value).toBigIntExact().map { intVal =>
        if (intVal < 0 || intVal > MAX_UINT64) {
          throw new JsonFormatException(s"Out of range uint64 value: $value")
        }
        PLong(intVal.longValue())
      } getOrElse {
        throw new JsonFormatException(s"Not an uint64 value: $value")
      }
    }
  }

}
