package io.kf.etl.transform

import org.apache.spark.sql.types._

import scalapb.descriptors.{Descriptor, FieldDescriptor, ScalaType}

object ScalaPB2SparkStructType {
  private def toSparkSQLType(fd:FieldDescriptor): DataType = {

    val basicType: DataType =
      fd.scalaType match {
        case ScalaType.Boolean => BooleanType
        case ScalaType.String => StringType
        case ScalaType.Int => IntegerType
        case ScalaType.Long => LongType
        case ScalaType.Double => DoubleType
        case ScalaType.Message(descriptor) => parseDescriptor(descriptor)
        case ScalaType.ByteString => BinaryType
        case ScalaType.Float => FloatType
        case _ => StringType
      }

    if(fd.isRepeated)
      ArrayType(basicType)
    else
      basicType
  }

  private def parseFieldDescriptor(fd: FieldDescriptor): StructField = {
//    StructField(fd.name, toSparkSQLType(fd), fd.isOptional)
    /**
      * when scalapb handles fields which contain undercore in the name, all of the underscores are removed
      * so here use "fd.asProto.jsonName.get" as StructField name
      */
    StructField(fd.asProto.jsonName.get, toSparkSQLType(fd), fd.isOptional)
  }

  def parseDescriptor(descriptor: Descriptor): StructType = {
    StructType(descriptor.fields.map(parseFieldDescriptor(_)))
  }
}
