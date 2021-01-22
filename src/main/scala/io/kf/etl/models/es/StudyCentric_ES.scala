package io.kf.etl.models.es

final case class StudyCentric_ES(
                                  kf_id: Option[String] = None,
                                  name: Option[String] = None,
                                  external_id: Option[String] = None,
                                  data_access_authority: Option[String] = None,
                                  code: Option[String] = None,
                                  domain: Option[String] = None,
                                  program: Option[String] = None,
                                  participant_count: Option[Long] = None,
                                  file_count: Option[Long] = None,
                                  family_count: Option[Long] = None
                                )
