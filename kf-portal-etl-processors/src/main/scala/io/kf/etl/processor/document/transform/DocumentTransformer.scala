package io.kf.etl.processor.document.transform

import io.kf.etl.dbschema.{TGraphPath, TPhenotype}
import io.kf.etl.processor.common.ProcessorCommonDefinitions._
import io.kf.etl.model._
import io.kf.etl.processor.document.context.DocumentContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

class DocumentTransformer(val context: DocumentContext) {

  def transform(input: DatasetsFromDBTables): Dataset[FileCentric] = {
    registerSparkTempViewsForPGTables(input)
    val par2gf = mapParticipantAndGenomicFile().cache()
    val participants = buildParticipants(input, par2gf)
    buildFiles(input, participants, par2gf)
  }

  private def computeHPO(hpo: DS_GRAPHPATH):Broadcast[Map[String, Seq[String]]] = {

    import context.sparkSession.implicits._
    context.sparkSession.sparkContext.broadcast(

      hpo.groupByKey(_.term1).mapGroups((term, iterator) => {
        val list = iterator.toList

        HPOReference(
          term = term.toString,
          ancestors = {
            list.tail.foldLeft((list(0), new ListBuffer[TGraphPath])){ (tuple, curr) => {
              if(curr.distance < tuple._1.distance){
                tuple._2.append(curr)
                (tuple._1, tuple._2)
              }
              else {
                tuple._2.append(tuple._1)
                (curr, tuple._2)
              }
            }}._2.map(_.term2.toString)
          }
        )
      }).collect().map(hporef => {
        (hporef.term, hporef.ancestors)
      }).toMap
    )
  }

  private def registerSparkTempViewsForPGTables(all: DatasetsFromDBTables) = {
    all.study.createOrReplaceGlobalTempView("ST")
    all.participant.createOrReplaceGlobalTempView("PAR")
    all.demographic.createOrReplaceGlobalTempView("DG")
    all.sample.createOrReplaceGlobalTempView("SA")
    all.aliquot.createOrReplaceGlobalTempView("AL")
    all.sequencingExperiment.createOrReplaceGlobalTempView("SE")
    all.diagnosis.createOrReplaceGlobalTempView("DI")
    all.phenotype.createOrReplaceGlobalTempView("PT")
    all.outcome.createOrReplaceGlobalTempView("OC")
    all.genomicFile.createOrReplaceGlobalTempView("GF")
    all.workflow.createOrReplaceGlobalTempView("WF")
    all.familyRelationship.createOrReplaceGlobalTempView("FR")
    all.participantAlis.createOrReplaceGlobalTempView("PA")
    all.workflowGenomicFile.createOrReplaceGlobalTempView("WG")
  }

  private def buildFamily(participant: Participant, relatives: Seq[FamilyMemberRelation]): Participant = {

    def buildFamilyData(composition:String, fmr: FamilyMemberRelation): FamilyData = {
      FamilyData(
        availableDataTypes = participant.availableDataTypes.intersect(fmr.relative.availableDataTypes),
        composition = composition,
        sharedHpoIds = {
          participant.phenotype.flatMap(pt => {
            pt.hpo match {
              case None => None
              case Some(hpo) => {
                Some(hpo.hpoIds)
              }
            }
          }).toList.flatten.intersect(
            fmr.relative.phenotype.flatMap(pt => {
              pt.hpo match {
                case None => None
                case (Some(hpo)) => {
                  Some(hpo.hpoIds)
                }
              }
            }).toList.flatten
          )
        }
      )
    }

    val father_mother = ".*(father|mother).*".r
    val twin = ".*twin.*".r

    relatives.size match {
      case 0 => participant
      case _ => {
        val familyData =
          relatives.map(relative => {
            relative.relation match {
              case Some(relation) => {
                relation.toLowerCase match {
                  case father_mother(c) => {
                    ("parent", buildFamilyData("partial_trio", relative))
                  }
                  case twin(c) => {
                    ("twin", buildFamilyData("twin", relative))
                  }
                  case _ => {
                    ("other",buildFamilyData("other", relative))
                  }
                }

              }
              case None => {
                // by default is "other"
                ("other",buildFamilyData("other", relative))
              }
            }
          }).groupBy(_._1).flatMap(tuple => {
            tuple._1 match {
              case "parent" => {
                tuple._2.size match {
                  case 1 => tuple._2.map(_._2)
                  case _ => {
                    val fds = tuple._2.map(_._2)
                    Seq(
                      fds.tail.foldLeft(fds(0).copy(composition = "complete_trio")){(left, right) => {
                        left.copy(
                          availableDataTypes = left.availableDataTypes.intersect(right.availableDataTypes),
                          sharedHpoIds = left.sharedHpoIds.intersect(right.sharedHpoIds)
                        )
                      }}
                    )
                  }
                }
              }
              case _ => tuple._2.map(_._2)
            }
          }).toSeq

        val familySharedHPOIds =
          familyData.tail.foldLeft(familyData(0).sharedHpoIds){(seq, fd) => {
            seq.intersect(fd.sharedHpoIds)
          }}

        participant.copy(
          family = Some(
            Family(
              familyId = participant.family.get.familyId,
              familyData = familyData,
              familyMembers = {
                relatives.map(p => {
                  FamilyMember(
                    kfId = p.relative.kfId,
                    uuid = p.relative.uuid,
                    createdAt = p.relative.createdAt,
                    modifiedAt = p.relative.modifiedAt,
                    isProband = p.relative.isProband,
                    availableDataTypes = p.relative.availableDataTypes,
                    phenotype = p.relative.phenotype.map(pt => {
                      pt.copy(hpo = Some(
                        pt.hpo.get.copy(sharedHpoIds = familySharedHPOIds)
                      ))
                    }),
                    studies = p.relative.studies,
                    race = p.relative.race,
                    ethnicity = p.relative.ethnicity,
                    relationship = p.relation
                  )
                })
              }
            )
          )
        )
      }
    }
  }

  private def buildParticipants(all: DatasetsFromDBTables, par2gf: Dataset[ParticipantToGenomicFiles] ):Dataset[Participant] = {
    import context.sparkSession.implicits._

    val hpoRefs = computeHPO(all.graphPath)

    val mergedParticipants = merge_Demographic_Diagnosis_Phenotype_Study_into_Participant(all, hpoRefs).cache()

    val family_relations = familyMemberRelationship(mergedParticipants, all.familyRelationship)

    val samples = buildSample(all)

    mergedParticipants.joinWith(par2gf, col("kfId"), "left").map(tuple => {
      Option(tuple._2) match {
        case Some(files) => tuple._1.copy(availableDataTypes = files.dataTypes)
        case None => tuple._1
      }
    }).joinWith(family_relations, col("kfId"), "left").groupByKey(tuple => {
      tuple._1.kfId
    }).mapGroups((id, iterator) => {
      val list = iterator.toList.filter(_._2 != null)

      buildFamily(list(0)._1, list.map(_._2))

    }).joinWith(samples, col("kfId"), "left").groupByKey(_._1.kfId).mapGroups((parId, iterator) => {
      val list = iterator.toList.filter(_._2!= null)
      list(0)._1.copy(samples = {
        list.flatMap(_._2.samples)
      })
    })

  }

  private def merge_Demographic_Diagnosis_Phenotype_Study_into_Participant(all: DatasetsFromDBTables, hpoRefs: Broadcast[Map[String, Seq[String]]]): Dataset[Participant] = {
    import context.sparkSession.implicits._

    val participant_demographic =
      all.participant.joinWith(all.demographic, all.participant.col("kfId") === all.demographic.col("participantId"), "left").map(tuple => {
      val participant = Participant(
        kfId = tuple._1.kfId,
        uuid = tuple._1.uuid,
        createdAt = tuple._1.createdAt,
        modifiedAt = tuple._1.modifiedAt
      )
      Option(tuple._2) match {
        case Some(dg) => participant.copy(
          family = tuple._1.familyId match {
            case Some(id) => Some(Family(familyId = id))
            case None => None
          },
          isProband = tuple._1.isProband,
          consentType = tuple._1.consentType,
          race = tuple._2.race,
          ethnicity = tuple._2.ethnicity,
          gender = tuple._2.gender
        )
        case None => participant
      }
    })


    val participant_diagnosis =
      all.participant.joinWith(all.diagnosis, all.participant.col("kfId") === all.diagnosis.col("participantId"), "left").groupByKey(_._1.kfId).mapGroups((parId, iterator) => {
        Participant(
          kfId = parId,
          uuid = "placeholder",
          createdAt = "placeholder",
          modifiedAt = "placeholder",
          diagnoses = {
            iterator.collect{
              case tuple if(tuple._2 != null) => {
                val tdia = tuple._2
                Diagnosis(
                  kfId = tdia.kfId,
                  uuid = tdia.uuid,
                  createdAt = tdia.createdAt,
                  modifiedAt = tdia.modifiedAt,
                  diagnosis = tdia.diagnosis,
                  ageAtEventDays = tdia.ageAtEventDays,
                  tumorLocation = tdia.tumorLocation,
                  diagnosisCategory = tdia.diagnosisCategory
                )
              }
            }.toSeq
            })
          }
        )

    def collectPhenotype(pheotypes: Iterator[TPhenotype]): Option[Phenotype] = {
      val seq = pheotypes.toSeq

      seq.size match {
        case 0 => None
        case _ => {
          Some(
            {
              case class DataHolder(
                                     ageAtEventDays: ListBuffer[Long] = new ListBuffer[Long](),
                                     createdAt: ListBuffer[String] = new ListBuffer[String](),
                                     modifiedAt: ListBuffer[String] = new ListBuffer[String](),
                                     observed: ListBuffer[String] = new ListBuffer[String](),
                                     phenotype: ListBuffer[String] = new ListBuffer[String](),
                                     negative: ListBuffer[String] = new ListBuffer[String](),
                                     positive: ListBuffer[String] = new ListBuffer[String]())

              val data =
                seq.foldLeft(DataHolder())((dh, tpt) => {
                  tpt.ageAtEventDays match {
                    case Some(value) => dh.ageAtEventDays.append(value)
                    case None =>
                  }
                  dh.createdAt.append(tpt.createdAt)
                  dh.modifiedAt.append(tpt.modifiedAt)
                  tpt.phenotype match {
                    case Some(value) => dh.phenotype.append(value)
                    case None =>
                  }
                  tpt.hpoId match {
                    case Some(value) => {
                      tpt.observed match {
                        case Some(o) => {
                          if(o.equals("positive")) {
                            dh.positive.append(value)
                            dh.observed.append(o)
                          }
                          else if(o.equals("negative")) {
                            dh.negative.append(value)
                            dh.observed.append(o)
                          }
                          else
                            println(s"the value -${o}- in observed is not supported")
                        }
                        case None => println("hpo_id exists, but observed is missing!")
                      }
                    }
                    case None => println("no hpo_id in Phenotype!")
                  }
                  dh
                })

              val ancestors =
                data.positive.flatMap(id => {
                  hpoRefs.value.get(id).toList.flatten
                })

              Phenotype(
                Some(
                  HPO(
                    ageAtEventDays = data.ageAtEventDays,
                    createdAt = data.createdAt,
                    modifiedAt = data.modifiedAt,
                    observed = data.observed,
                    phenotype = data.phenotype,
                    negativeHpoIds = data.negative,
                    hpoIds = data.positive,
                    ancestralHpoIds = ancestors
                  )
                )
              )
            }
          )
        }
      }
    }

    val participant_phenotype =
      all.participant.joinWith(all.phenotype, all.participant.col("kfId") === all.phenotype.col("participantId"), "left").groupByKey(_._1.kfId).mapGroups((parId, iterator) => {
        Participant(
          kfId = parId,
          uuid = "placeholder",
          createdAt = "placeholder",
          modifiedAt = "placeholder",
          phenotype = collectPhenotype(
            iterator.collect{
              case tuple if(tuple._2 != null) => tuple._2
            }
          )
        )
      })

    val participant_study =
      all.participant.joinWith(all.study, all.participant.col("kfId") === all.study.col("participantId"), "left").groupByKey(_._1.kfId).mapGroups((parId, iterator) => {
        Participant(
          kfId = parId,
          uuid = "placeholder",
          createdAt = "placeholder",
          modifiedAt = "placeholder",
          studies = {
            iterator.collect{
              case tuple if(tuple._2 != null) => {
                val ts = tuple._2
                Study(
                  kfId = ts.kfId,
                  uuid = ts.uuid,
                  createdAt = ts.createdAt,
                  modifiedAt = ts.modifiedAt,
                  dataAccessAuthority = ts.dataAccessAuthority,
                  externalId = ts.externalId,
                  version = ts.version,
                  name = ts.name,
                  attribution = ts.attribution
                )
              }
            }.toSeq
          }
        )
      })

    participant_demographic.joinWith(participant_diagnosis, col("kfId"), "left").map(tuple => {
      Option(tuple._2) match {
        case Some(p) => tuple._1.copy(diagnoses = p.diagnoses)
        case None => tuple._1
      }

    }).joinWith(participant_phenotype, col("kfId"), "left").map(tuple => {
      Option(tuple._2) match {
        case Some(p) => tuple._1.copy(phenotype = p.phenotype)
        case None => tuple._1
      }
    }).joinWith(participant_study, col("kfId"), "left").map(tuple => {
      Option(tuple._2) match {
        case Some(p) => tuple._1.copy(studies = p.studies)
        case None => tuple._1
      }
    })

  }

  private def buildSample(all: DatasetsFromDBTables): Dataset[ParticipantToSamples] = {
    import context.sparkSession.implicits._

    all.sample.joinWith(all.aliquot, all.sample.col("kfId") === all.aliquot.col("sampleId"), "left").groupByKey(_._1.kfId).mapGroups((sampleId, iterator) => {
      val list = iterator.toList
      val tsample = list(0)._1
      (
        tsample.participantId,
        Sample(
          kfId = tsample.kfId,
          uuid = tsample.uuid,
          composition = tsample.composition,
          tissueType = tsample.tissueType,
          tumorDescriptor = tsample.tumorDescriptor,
          anatomicalSite = tsample.anatomicalSite,
          ageAtEventDays = tsample.ageAtEventDays,
          aliquots = {
            list.collect{
              case tuple if(tuple._2 != null) => {
                val taliquot = tuple._2
                Aliquot(
                  kfId = taliquot.kfId,
                  uuid = taliquot.uuid,
                  createdAt = taliquot.createdAt,
                  modifiedAt = taliquot.modifiedAt,
                  shipmentOrigin = taliquot.shipmentOrigin,
                  shipmentDestination = taliquot.shipmentDestination,
                  shipmentDate = taliquot.shipmentDate,
                  analyteType = taliquot.analyteType,
                  concentration = taliquot.concentration,
                  volume = taliquot.volume
                )
              }
            }
          }
        )
      )
    }).groupByKey(_._1).mapGroups((parId, iterator) => {
      ParticipantToSamples(
        parId,
        iterator.map(_._2).toSeq
      )
    })
  }

  private def mapParticipantAndGenomicFile(): Dataset[ParticipantToGenomicFiles] = {
    val sql =
      """
         select PAR.kfId, AA.gfId, AA.dataType from PAR left join (
           (select SA.participantId as kfId, BB.gfId, BB.dataType from SA left join (
             (select AL.sampleId as kfId, CC.gfId, CC.dataType from AL left join (
                (select SE.aliquotId as kfId, GF.kfId as gfId, GF.dataType from SE left join GF on SE.kfId = GF.sequencingExperimentId) as CC
              ) on AL.kfId = CC.kfId) as BB
            ) on SA.kfId = BB.kfId ) as AA
         ) on PAR.kfId = AA.kfId
      """.stripMargin

    import context.sparkSession.implicits._
    context.sparkSession.sql(sql).groupByKey(_.getString(0)).mapGroups((par_id, iterator) => {

      val list = iterator.toList

      ParticipantToGenomicFiles(
        par_id,
        list.collect{
          case row if(row.getString(1) != null) => row.getString(1)
        },
        list.collect{
          case row if(row.getString(2) != null) => row.getString(1)
        }
      )
    })
  }

  /**
    * this method returns participant and his relatives, where a participant is represented by only kfId, a relative is represented by a instance of Participant. Plus also the relationship from a relative to a participant
    * @param left
    * @param right
    * @return
    */
  private def familyMemberRelationship(left: Dataset[Participant], right: DS_FAMILYRELATIONSHIP): Dataset[FamilyMemberRelation] = {
    import context.sparkSession.implicits._

    left.joinWith(right, left.col("kfId") === right.col("relativeId")).map(tuple => {

      FamilyMemberRelation(
        tuple._2.participantId,
        tuple._1,
        tuple._2.relativeToParticipantRelation
      )
    })
  }


  private def buildFiles(all:DatasetsFromDBTables, participants: Dataset[Participant],  par2gf: Dataset[ParticipantToGenomicFiles]): Dataset[FileCentric] = {
    import context.sparkSession.implicits._
    val file2SeqExps =
      all.genomicFile.joinWith(all.sequencingExperiment, all.sequencingExperiment.col("kfId") === all.genomicFile.col("sequencingExperimentId"), "left").groupByKey(_._1.kfId).mapGroups((fileId, iterator) => {

        GenomicFileToSeqExps(
          fileId,
          iterator.collect{
            case tuple if(tuple._2 != null) => {
              val tseq = tuple._2
              SequencingExperiment(
                kfId = tseq.kfId,
                uuid = tseq.uuid,
                createdAt = tseq.createdAt,
                modifiedAt = tseq.modifiedAt,
                experimentDate = tseq.experimentDate,
                experimentStrategy = tseq.experimentStrategy,
                center = tseq.center,
                libraryName = tseq.libraryName,
                libraryStrand = tseq.libraryStrand,
                isPairedEnd = tseq.isPairedEnd,
                platform = tseq.platform,
                instrumentModel = tseq.instrumentModel,
                maxInsertSize = tseq.maxInsertSize,
                meanInsertSize = tseq.meanInsertSize,
                minInsertSize = tseq.minInsertSize,
                meanDepth = tseq.meanDepth,
                meanReadLength = tseq.meanReadLength,
                totalReads = tseq.totalReads
              )
            }
          }.toSeq
        )
      })

    val file2Workflows =
      all.workflowGenomicFile.joinWith(all.workflow, all.workflowGenomicFile.col("workflowId") === all.workflow.col("kfId")).groupByKey(_._1.genomicFileId).mapGroups((fileId, iterator) => {
        GenomicFileToWorkflows(
          fileId,
          iterator.collect{
            case tuple if(tuple._2 != null) => {
              val tflow = tuple._2
              Workflow(
                kfId = tflow.kfId,
                uuid = tflow.uuid,
                createdAt = tflow.createdAt,
                modifiedAt = tflow.modifiedAt,
                taskId = tflow.taskId,
                name = tflow.name,
                version = tflow.version,
                githubUrl = tflow.githubUrl
              )
            }
          }.toSeq
        )
      })

    val file2Participants =
      par2gf.joinWith(participants, col("kfId")).flatMap(tuple => {
        tuple._1.fielIds.map(id => (id, tuple._2))
      }).groupByKey(_._1).mapGroups((fileId, iterator) => {
        GenomicFileToParticipants(
          fileId,
          iterator.map(_._2).toSeq
        )
      })

    all.genomicFile.joinWith(file2SeqExps, col("kfId")).groupByKey(_._1.kfId).mapGroups((fileId, iterator) => {
      val list = iterator.toList

      val tgf = list(0)._1
      FileCentric(
        kfId = tgf.kfId,
        uuid = tgf.uuid,
        createdAt = tgf.createdAt,
        modifiedAt = tgf.modifiedAt,
        fileName = tgf.fileName,
        dataType = tgf.dataType,
        fileFormat = tgf.fileFormat,
        fileUrl = tgf.fileUrl,
        controlledAccess = tgf.controlledAccess,
        md5Sum = tgf.md5Sum,
        sequencingExperiments = iterator.flatMap(_._2.exps).toSeq
      )
    }).joinWith(file2Workflows, col("kfId")).groupByKey(_._1.kfId).mapGroups((fileId, iterator) => {

      val list = iterator.toList
      val fc = list(0)._1

      fc.copy(
        workflow = {
          iterator.flatMap(tuple => {
            tuple._2.flows
          }).toSeq
        }
      )
    }).joinWith(file2Participants, col("kfId")).groupByKey(_._1.kfId).mapGroups((fileId, iterator) => {
      val list = iterator.toList
      val fc = list(0)._1
      fc.copy(
        participants = {
          iterator.flatMap(_._2.participants).toSeq
        }
      )
    })

  }

}
