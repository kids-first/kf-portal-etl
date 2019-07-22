package io.kf.etl.processors.common.step.impl

import io.kf.etl.es.models.{FamilyComposition_ES, FamilyMember_ES, Family_ES, Participant_ES}
import io.kf.etl.processors.common.ProcessorCommonDefinitions.EntityDataSet
import io.kf.etl.processors.common.step.StepExecutable
import io.kf.etl.processors.common.step.context.StepContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

class MergeFamily(override val ctx: StepContext) extends StepExecutable[Dataset[Participant_ES], Dataset[Participant_ES]] {
  override def process(participants: Dataset[Participant_ES]): Dataset[Participant_ES] = {
    import ctx.spark.implicits._

    val availableDataTypesBroadcast = calculateAvailableDataTypes(ctx.entityDataset)
    val flattenedFamilyRelationshipBroadcast = getFlattenedFamilyRelationship(ctx.entityDataset)

    participants
      .groupByKey(_.familyId)
      .flatMapGroups((family_id, iterator) => {
        MergeFamily.deduceFamilyCompositions(family_id, iterator.toSeq, flattenedFamilyRelationshipBroadcast, availableDataTypesBroadcast)
      })
  }

  def calculateAvailableDataTypes(entityDataset: EntityDataSet): Broadcast[Map[String, Seq[String]]] = {
    import ctx.spark.implicits._
    import entityDataset._
    val datatypeByParticipant = participants
      .join(biospecimens, participants("kfId") === biospecimens("participantId"))
      .join(biospecimenGenomicFiles, biospecimens("kfId") === biospecimenGenomicFiles("biospecimenId"))
      .join(genomicFiles, biospecimenGenomicFiles("genomicFileId") === genomicFiles("kfId"))
      .select(participants("kfId") as "participantId", genomicFiles("dataType") as "dataType")
      .groupBy($"participantId").agg(collect_list("dataType") as "dataTypes")
      .as[(String, Seq[String])]
      .collect()
      .toMap

    val distinctDatatypeByParticipant = datatypeByParticipant.map { case (k, v) => k -> v.distinct }

    ctx.spark.sparkContext.broadcast[Map[String, Seq[String]]](distinctDatatypeByParticipant)

  }

  def getFlattenedFamilyRelationship(entityDataset: EntityDataSet): Broadcast[Map[String, Seq[(String, String)]]] = {
    /**
      * flattenedFamilyRelationship is a map
      * key  : participant id
      * value: Set of relation types as lowercase strings
      */
    import ctx.spark.implicits._

    val g: Dataset[(String, Seq[(String, String)])] = entityDataset.familyRelationships
      .flatMap(tf => {
        Seq(
          (tf.participant1, (tf.participant2, tf.participant2ToParticipant1Relation)),
          (tf.participant2, (tf.participant1, tf.participant1ToParticipant2Relation))
        )
      })
      .groupByKey(_._1)
      .mapGroups((participant_id, iterator) => (participant_id.get, iterator.collect { case (_, (Some(participant), Some(relation))) => (participant, relation.toLowerCase) }.toSeq))

    ctx.spark.sparkContext.broadcast(g.collect().toMap)


  }
}

object MergeFamily {


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
    val familyRelationship = familyRelationship_broadcast.value
    val mapOfAvailableDataTypes = availableDataTypes_broadcast.value

    familyId match {

      case None => family.map(participant => {

        val members = Seq(participant)
        val sharedHpoIds = getSharedHpoIds(members)
        val family_availableDataTypes = getAvailableDataTypes(members, mapOfAvailableDataTypes)

        val composition = participant.isProband match {
          case Some(true) => "proband-only"
          case _ => "other"
        }

        val familyMembers = Seq(
          getFamilyMemberFromParticipant(participant, "", sharedHpoIds, mapOfAvailableDataTypes)
        )

        val familyComposition =
          FamilyComposition_ES(
            composition = Some(composition),
            sharedHpoIds = sharedHpoIds,
            availableDataTypes = family_availableDataTypes,
            familyMembers = familyMembers
          )

        val family =
          Family_ES(
            familyCompositions = Seq(familyComposition)
          )

        participant.copy(
          family = Some(family),
          availableDataTypes = getAvailableDataTypes(Seq(participant), mapOfAvailableDataTypes)
        )

      })

      case Some(_) =>
        val participantById = family.map(p => p.kfId.get -> p).toMap
        val sharedHpoIds = getSharedHpoIds(family)
        val familyAvailableDataTypes = getAvailableDataTypes(family, mapOfAvailableDataTypes)
        val probandIds = getProbandIds(family)

        val familyComposition = getFamilyComposition(familyRelationship, probandIds)
        family.map { participant =>
          val familyMembersWithRelationship: Seq[FamilyMember_ES] = familyRelationship.getOrElse(participant.kfId.get, Nil).collect {
            case (relationParticipantId, relation) if participantById.contains(relationParticipantId) => getFamilyMemberFromParticipant(participantById(relationParticipantId), relation, sharedHpoIds, mapOfAvailableDataTypes)
          }

          val otherFamilyMembers = family.collect { case p if p.kfId != participant.kfId && !familyMembersWithRelationship.exists(member => member.kfId == p.kfId) => getFamilyMemberFromParticipant(p, "member", sharedHpoIds, mapOfAvailableDataTypes) }

          val composition =
            FamilyComposition_ES(
              composition = Some(familyComposition.toString),
              sharedHpoIds = sharedHpoIds,
              availableDataTypes = familyAvailableDataTypes,
              familyMembers = familyMembersWithRelationship ++ otherFamilyMembers
            )
          val participantAvailableDatatype = getAvailableDataTypes(Seq(participant), mapOfAvailableDataTypes)

          participant.copy(
            family = Some(Family_ES(familyId = participant.familyId, familyCompositions = Seq(composition))),
            availableDataTypes = participantAvailableDatatype
          )

        }

    }
  }

  def getProbandIds(family: Seq[Participant_ES]): Seq[String] = {
    family.collect { case p if p.isProband.exists(proband => proband) => p.kfId.get }
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

    val availableDataTypes = mapOfAvailableDataTypes.get(participant.kfId.get) match {
      case Some(types) => types.toSet.toSeq
      case None => Seq.empty
    }


    FamilyMember_ES(
      kfId = participant.kfId,
      isProband = participant.isProband,
      availableDataTypes = availableDataTypes,
      phenotype = participant.phenotype,
      race = participant.race,
      ethnicity = participant.ethnicity,
      relationship = Some(relationship)
    )
  }

  def getAvailableDataTypes(participants: Seq[Participant_ES], mapOfAvailableDataTypes: Map[String, Seq[String]]): Seq[String] = {

    val participantKeys: Seq[String] = participants.flatMap(_.kfId)
    mapOfAvailableDataTypes.filterKeys(participantKeys.contains).values.flatten.toSet.toSeq

  }

  def getSharedHpoIds(participants: Seq[Participant_ES]): Seq[String] = {
    participants.tail.foldLeft(
      participants.head.phenotype match {
        case Some(pt) => pt.hpoPhenotypeObserved
        case None => Seq.empty[String]
      }
    ) { (pts, participant) => {
      participant.phenotype match {
        case Some(pt) => pt.hpoPhenotypeObserved
        case None => Seq.empty[String]
      }
    }
    }
  }

}