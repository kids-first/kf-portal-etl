package io.kf.etl.models.duocode

case class DuoCode(
                    id: String,
                    shorthand: Option[String] = None,
                    label: Option[String] = None,
                    description: Option[String] = None
                  ) {
  override def toString: String = {
    label match {
      case Some(l) => s"${l} ($id)"
      case None => s"$id"
    }
  }
}