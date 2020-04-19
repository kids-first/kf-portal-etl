package io.kf.etl.processors.participantcommon.transform.step

import io.kf.etl.models.es.{FamilyComposition_ES, FamilyMember_ES, Family_ES, Participant_ES}
import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions.collect_list
import org.apache.spark.sql.{Dataset, SparkSession}

object MergeFamily {
  def apply(entityDataset: EntityDataSet, participants: Dataset[Participant_ES])(implicit spark: SparkSession): Dataset[Participant_ES] = {
    import spark.implicits._

    val availableDataTypesBroadcast = calculateAvailableDataTypes(entityDataset)
    val flattenedFamilyRelationshipBroadcast = getFlattenedFamilyRelationship(entityDataset)

    participants
      .groupByKey(_.family_id)
      .flatMapGroups((family_id, iterator) => {
        deduceFamilyCompositions(family_id, iterator.toSeq, flattenedFamilyRelationshipBroadcast, availableDataTypesBroadcast)
      })
  }

  def calculateAvailableDataTypes(entityDataset: EntityDataSet)(implicit spark: SparkSession): Broadcast[Map[String, Seq[String]]] = {
    import entityDataset._
    import spark.implicits._
    val datatypeByParticipant = participants
      .join(biospecimens, participants("kf_id") === biospecimens("participant_id"))
      .join(biospecimenGenomicFiles, biospecimens("kf_id") === biospecimenGenomicFiles("biospecimen_id"))
      .join(genomicFiles, biospecimenGenomicFiles("genomic_file_id") === genomicFiles("kf_id"))
      .select(participants("kf_id") as "participant_id", genomicFiles("data_type") as "data_type")
      .groupBy($"participant_id").agg(collect_list("data_type") as "data_types")
      .as[(String, Seq[String])]
      .collect()
      .toMap

    val distinctDatatypeByParticipant = datatypeByParticipant.map { case (k, v) => k -> v.distinct }

    spark.sparkContext.broadcast[Map[String, Seq[String]]](distinctDatatypeByParticipant)

  }

  def getFlattenedFamilyRelationship(entityDataset: EntityDataSet)(implicit spark: SparkSession): Broadcast[Map[String, Seq[(String, String)]]] = {
    /**
      * flattenedFamilyRelationship is a map
      * key  : participant id
      * value: Set of relation types as lowercase strings
      */
    import spark.implicits._

    val g: Dataset[(String, Seq[(String, String)])] = entityDataset.familyRelationships
      .flatMap(tf => {
        Seq(
          (tf.participant1, (tf.participant2, tf.participant2_to_participant1_relation)),
          (tf.participant2, (tf.participant1, tf.participant1_to_participant2_relation))
        )
      })
      .groupByKey(_._1)
      .mapGroups((participant_id, iterator) => (participant_id.get, iterator.collect { case (_, (Some(participant), Some(relation))) => (participant, relation.toLowerCase) }.toSeq))

    spark.sparkContext.broadcast(g.collect().toMap)


  }

  class ProbandMissingInFamilyException extends Exception("Family has no proband child!")

  trait FamilyType extends Comparable[FamilyType] {
    def c: Int

    override def compareTo(other: FamilyType): Int = c.compareTo(other.c)

  }

  object FamilyType {
    def max(f1: FamilyType, f2: FamilyType): FamilyType = Seq(f1, f2).maxBy(_.c)
  }

  case object Duo extends FamilyType {
    override val c: Int = 200

    override def toString: String = "duo"
  }

  case object DuoPlus extends FamilyType {
    override val c: Int = 201

    override def toString: String = "duo+"
  }

  case object Trio extends FamilyType {
    override val c: Int = 300

    override def toString: String = "trio"
  }

  case object TrioPlus extends FamilyType {
    override val c: Int = 301

    override def toString: String = "trio+"
  }

  case object Other extends FamilyType {
    override val c: Int = 10

    override def toString: String = "other"
  }

  case object ProbandOnly extends FamilyType {
    override val c: Int = 0

    override def toString: String = "proband-only"
  }

  /*
    familyRelationship map:
    key  : participant id
    value: list of relationship in the family, for example, father, mother, child
   */

  def deduceFamilyCompositions(familyId: Option[String],
                               family: Seq[Participant_ES],
                               familyRelationship_broadcast: Broadcast[Map[String, Seq[(String, String)]]],
                               availableDataTypes_broadcast: Broadcast[Map[String, Seq[String]]]
                              ): Seq[Participant_ES] = {
    val mapOfRelationships = familyRelationship_broadcast.value
    val mapOfAvailableDataTypes = availableDataTypes_broadcast.value

    familyId match {

      case None => family.map(participant => {

        val members = Seq(participant)
        val sharedHpoIds = getSharedHpoIds(members)
        val family_availableDataTypes = getAvailableDataTypes(members, mapOfAvailableDataTypes)

        val composition = participant.is_proband match {
          case Some(true) => "proband-only"
          case _ => "other"
        }

        val familyMembers = Seq(
          getFamilyMemberFromParticipant(participant, "", sharedHpoIds, mapOfAvailableDataTypes)
        )

        val familyComposition =
          FamilyComposition_ES(
            composition = Some(composition),
            shared_hpo_ids = sharedHpoIds,
            available_data_types = family_availableDataTypes,
            family_members = familyMembers
          )

        val family =
          Family_ES(
            family_compositions = Seq(familyComposition)
          )

        participant.copy(
          family = Some(family),
          available_data_types = getAvailableDataTypes(Seq(participant), mapOfAvailableDataTypes)
        )

      })

      case Some(_) =>
        val participantById = family.map(p => p.kf_id.get -> p).toMap
        val sharedHpoIds = getSharedHpoIds(family)
        val familyAvailableDataTypes = getAvailableDataTypes(family, mapOfAvailableDataTypes)
        val probandIds = getProbandIds(family)
        val familyRelationship = mapOfRelationships.filterKeys(participantById.contains)
        val familyComposition = getFamilyComposition(familyRelationship, probandIds)
        family.map { participant =>
          val familyMembersWithRelationship: Seq[FamilyMember_ES] = mapOfRelationships.getOrElse(participant.kf_id.get, Nil).collect {
            case (relationParticipantId, relation) if participantById.contains(relationParticipantId) => getFamilyMemberFromParticipant(participantById(relationParticipantId), relation, sharedHpoIds, mapOfAvailableDataTypes)
          }

          val otherFamilyMembers = family.collect { case p if p.kf_id != participant.kf_id && !familyMembersWithRelationship.exists(member => member.kf_id == p.kf_id) => getFamilyMemberFromParticipant(p, "member", sharedHpoIds, mapOfAvailableDataTypes) }

          val composition =
            FamilyComposition_ES(
              composition = Some(familyComposition.toString),
              shared_hpo_ids = sharedHpoIds,
              available_data_types = familyAvailableDataTypes,
              family_members = familyMembersWithRelationship ++ otherFamilyMembers
            )
          val participantAvailableDatatype = getAvailableDataTypes(Seq(participant), mapOfAvailableDataTypes)

          participant.copy(
            family = Some(Family_ES(family_id = participant.family_id, family_compositions = Seq(composition))),
            available_data_types = participantAvailableDatatype
          )

        }

    }
  }

  def getProbandIds(family: Seq[Participant_ES]): Seq[String] = {
    family.collect { case p if p.is_proband.exists(proband => proband) => p.kf_id.get }
  }

  def getFamilyComposition(familyRelationship: Map[String, Seq[(String, String)]], probands: Seq[String]): FamilyType = {
    val membersCount = familyRelationship.keys.size

    val filteredRelationship = if (probands.isEmpty) familyRelationship else familyRelationship.filterKeys(probands.contains(_))
    filteredRelationship.values.foldLeft(ProbandOnly: FamilyType) { (previous, relations) =>
      val current = relations.map(_._2).toSet match {
        case l if l.contains("mother") && l.contains("father") => if (membersCount > 3) TrioPlus else Trio
        case l if l.contains("mother") || l.contains("father") => if (membersCount > 2) DuoPlus else Duo
        case l if l.isEmpty => previous
        case _ => Other
      }

      FamilyType.max(current, previous)
    }
  }

  def getFamilyMemberFromParticipant(participant: Participant_ES,
                                     relationship: String,
                                     familySharedHpoIds: Seq[String],
                                     mapOfAvailableDataTypes: Map[String, Seq[String]]): FamilyMember_ES = {

    val availableDataTypes = mapOfAvailableDataTypes.get(participant.kf_id.get) match {
      case Some(types) => types.toSet.toSeq
      case None => Seq.empty
    }


    FamilyMember_ES(
      kf_id = participant.kf_id,
      is_proband = participant.is_proband,
      available_data_types = availableDataTypes,
      phenotype = participant.phenotype,
      diagnoses = participant.diagnoses,
      gender = participant.gender,
      race = participant.race,
      ethnicity = participant.ethnicity,
      relationship = Some(relationship)
    )
  }

  def getAvailableDataTypes(participants: Seq[Participant_ES], mapOfAvailableDataTypes: Map[String, Seq[String]]): Seq[String] = {

    val participantKeys: Seq[String] = participants.flatMap(_.kf_id)
    mapOfAvailableDataTypes.filterKeys(participantKeys.contains).values.flatten.toSet.toSeq

  }

  private def extractHpoObserved(participant: Participant_ES) = {
    participant.phenotype.flatMap(_.hpo_phenotype_observed).toSet
  }

  def getSharedHpoIds(participants: Seq[Participant_ES]): Seq[String] = {
    if (participants.isEmpty) Nil
    else {
      participants.tail.foldLeft(extractHpoObserved(participants.head)) { (acc, participant) =>
        val hpoObserved: Set[String] = extractHpoObserved(participant)
        acc.intersect(hpoObserved)
      }.toSeq
    }
  }

}
