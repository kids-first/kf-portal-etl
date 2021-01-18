package io.kf.etl.models.internal

import org.apache.spark.sql.Encoder

case class StudyExtraParams(
                             kf_id: Option[String] = None,
                             study_title: Option[String] = None,
                             code: Option[String] = None,
                             domain: Option[String] = None,
                             program: Option[String] = None
                           )
object StudyExtraParams{
  implicit val encoder: Encoder[StudyExtraParams] = org.apache.spark.sql.Encoders.product[StudyExtraParams]
}