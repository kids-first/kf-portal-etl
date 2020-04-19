package io.kf.etl.models.dataservice

case class EFamilyRelationship(
                                kf_id: scala.Option[String] = None,
                                modified_at: scala.Option[String] = None,
                                created_at: scala.Option[String] = None,
                                participant1: scala.Option[String] = None,
                                participant2: scala.Option[String] = None,
                                participant1_to_participant2_relation: scala.Option[String] = None,
                                participant2_to_participant1_relation: scala.Option[String] = None,
                                visible: scala.Option[Boolean] = None
    )
