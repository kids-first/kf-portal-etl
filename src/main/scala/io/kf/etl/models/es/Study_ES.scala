package io.kf.etl.models.es

final case class Study_ES(
                           kf_id: scala.Option[String] = None,
                           attribution: scala.Option[String] = None,
                           name: scala.Option[String] = None,
                           version: scala.Option[String] = None,
                           external_id: scala.Option[String] = None,
                           release_status: scala.Option[String] = None,
                           data_access_authority: scala.Option[String] = None,
                           short_name: scala.Option[String] = None,
                           code: Option[String] = None,
                           domain: Seq[String] = Seq.empty,
                           program: Option[String] = None
                         )
