package io.kf.etl.models.dataservice

case class EFamilyRelationship(
    kfId: scala.Option[String] = None,
    modifiedAt: scala.Option[String] = None,
    createdAt: scala.Option[String] = None,
    participant1: scala.Option[String] = None,
    participant2: scala.Option[String] = None,
    participant1ToParticipant2Relation: scala.Option[String] = None,
    participant2ToParticipant1Relation: scala.Option[String] = None,
    visible: scala.Option[Boolean] = None
    )
