package io.kf.etl.models.es

final case class StudyCentric_ES(
                                  kf_id: Option[String] = None,
                                  name: Option[String] = None,
                                  external_id: Option[String] = None,
                                  data_access_authority: Option[String] = None,
                                  code: Option[String] = None,
                                  domain: Seq[String] = Nil,
                                  program: Option[String] = None,
                                  participant_count: Option[Long] = None,
                                  file_count: Option[Long] = None,
                                  family_count: Option[Long] = None,
                                  family_data: Option[Boolean] = None,
                                  experimental_strategy: Seq[String] = Nil,
                                  data_categories: Seq[String] = Nil,
                                  data_category_count: Seq[DataCategoryWCount_ES] = Nil
                                )
