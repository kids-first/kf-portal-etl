package io.kf.etl.models.duocode

case class DuoCode(
                id: String,
                shorthand: Option[String] = None,
                label: Option[String] = None,
                description: Option[String] = None
              )
