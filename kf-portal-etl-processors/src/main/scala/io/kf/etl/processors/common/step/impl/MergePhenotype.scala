package io.kf.etl.processors.common.step.impl

import io.kf.etl.es.models.{Participant_ES, Phenotype_ES}
import io.kf.etl.external.dataservice.entity.EPhenotype
import io.kf.etl.external.hpo.{GraphPath, OntologyTerm}
import io.kf.etl.model.utils.HPOReference
import io.kf.etl.processors.common.step.StepExecutable
import io.kf.etl.processors.filecentric.transform.steps.context.StepContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Dataset

import scala.collection.mutable.ListBuffer

class MergePhenotype(override val ctx: StepContext) extends StepExecutable[Dataset[Participant_ES], Dataset[Participant_ES]] {
  override def process(participants: Dataset[Participant_ES]): Dataset[Participant_ES] = {

    import ctx.spark.implicits._
    val hpoRefs = generateHpoRefs()
    val hpoTerms = generateHpoTerms()


    val transformedPhenotypes = ctx.entityDataset.phenotypes
        .filter(pt => pt.observed.isDefined)
        .map(pt => pt.copy(
          observed = Some(pt.observed.get.toLowerCase)
        ))

    participants.joinWith(
      transformedPhenotypes,
      participants.col("kfId") === transformedPhenotypes.col("participantId"),
      "left_outer"
    ).groupByKey(tuple => {
      tuple._1.kfId.get
    }).mapGroups((_, iterator) => {
      val seq = iterator.toSeq
      val participant = seq(0)._1

      val filteredSeq = seq.filter(_._2 != null)

      filteredSeq.size match {
        case 0 => participant
        case _ => {
          participant.copy(
            phenotype = MergePhenotype.collectPhenotype(
              seq.map(_._2),
              hpoRefs,
              hpoTerms
            )
          )//end of copy
        }
      }
    })//end of mapGroups
  }

  def generateHpoRefs(): Broadcast[Map[String, Seq[String]]] = {
    import ctx.spark.implicits._
    ctx.spark.sparkContext.broadcast(

      ctx.entityDataset.graphPath.groupByKey(_.term1).mapGroups((term, iterator) => {
        val list = iterator.toList

        HPOReference(
          term = term.toString,
          ancestors = {
            list.tail.foldLeft((list(0), new ListBuffer[GraphPath])){ (tuple, curr) => {
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
  }// end of generateHpoRefs

  def generateHpoTerms(): Broadcast[Map[String, String]] = {
    ctx.spark.sparkContext.broadcast(
      ctx.entityDataset.hpoTerms.collect.map(term=>(term.id, term.name)).toMap
    )
  }

}

object MergePhenotype {

  def collectPhenotype(phenotypes: Seq[EPhenotype],
                       hpoRefs:Broadcast[Map[String, Seq[String]]],
                       hpoTerms:Broadcast[Map[String, String]]
                      ): Option[Phenotype_ES] = {

    val seq = phenotypes.toSeq

    seq.size match {
      case 0 => None
      case _ => {
        Some(
          {
            case class DataHolder(
                                   ageAtEventDays: ListBuffer[Int] = new ListBuffer[Int](),
                                   createdAt: ListBuffer[String] = new ListBuffer[String](),
                                   modifiedAt: ListBuffer[String] = new ListBuffer[String](),
                                   observed: ListBuffer[String] = new ListBuffer[String](),
                                   hpoIdsNotObserved: ListBuffer[String] = new ListBuffer[String](),
                                   hpoIdsObserved: ListBuffer[String] = new ListBuffer[String](),
                                   hpoObservedText: ListBuffer[String] = new ListBuffer[String](),
                                   hpoIdPhenotype: ListBuffer[String] = new ListBuffer[String](),
                                   sourceTextPhenotype: ListBuffer[String] = new ListBuffer[String](),
                                   snomedIdsObserved: ListBuffer[String] = new ListBuffer[String](),
                                   snomedIdsNotObserved: ListBuffer[String] = new ListBuffer[String](),
                                   externalId: ListBuffer[String] = new ListBuffer[String]()
                                 )

            val data =
              seq.foldLeft(DataHolder())((dh, tpt) => {
                tpt.ageAtEventDays match {
                  case Some(value) => dh.ageAtEventDays.append(value)
                  case None =>
                }
                dh.createdAt.append(tpt.createdAt.get)
                dh.modifiedAt.append(tpt.modifiedAt.get)

                var hpoObserved: Boolean = false
                tpt.hpoIdPhenotype match {
                  case Some(hpo_id) => {
                    tpt.observed match {
                      case Some(o) => {
                        if(o.equals("positive")) {
                          dh.hpoIdsObserved.append(hpo_id)
                          dh.observed.append(o)

                          // Lookup hpoText and append if found
                          hpoTerms.value.get(hpo_id) match {
                            case Some(text) => dh.hpoObservedText.append(s"${text} (${hpo_id})")
                            case _ =>
                          }

                          hpoObserved = true
                        }
                        else if(o.equals("negative")) {
                          dh.hpoIdsNotObserved.append(hpo_id)
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


                var snomedObserved: Boolean = false
                tpt.snomedIdPhenotype match {
                  case Some(snome_id) => {
                    tpt.observed match {
                      case Some(o) => {
                        if(o.equals("positive")) {
                          dh.snomedIdsObserved.append(snome_id)
                          dh.observed.append(o)
                          snomedObserved = true
                        }
                        else if(o.equals("negative")) {
                          dh.snomedIdsNotObserved.append(snome_id)
                          dh.observed.append(o)
                        }
                        else
                          println(s"the value -${o}- in observed is not supported")
                      }
                      case None => println("snomedIdPhenotype exists, but observed is missing!")
                    }
                  }
                  case None =>
                }

                // Only append to source text in the positive case for observed
                if (hpoObserved || snomedObserved ) {
                  tpt.sourceTextPhenotype match {
                    case Some(source_text) => dh.sourceTextPhenotype.append(source_text)
                    case None =>  
                  }
                }

                tpt.externalId match {
                  case Some(external) => dh.externalId.append(external)
                  case None =>
                }

                dh
              })

            val ancestors =
              data.hpoIdsObserved.flatMap(id => {
                hpoRefs.value.get(id).toList.flatten
              }).toSet.toSeq

            Phenotype_ES(
              ageAtEventDays = data.ageAtEventDays.toSet.toSeq,
              ancestralHpoIds = ancestors.toSet.toSeq,
              externalId = data.externalId.toSet.toSeq,
              hpoPhenotypeNotObserved = data.hpoIdsNotObserved.toSet.toSeq,
              hpoPhenotypeObserved = data.hpoIdsObserved.toSet.toSeq,
              hpoPhenotypeObservedText = data.hpoObservedText.toSet.toSeq,
              snomedPhenotypeNotObserved = data.snomedIdsNotObserved.toSet.toSeq,
              snomedPhenotypeObserved = data.snomedIdsObserved.toSet.toSeq,
              sourceTextPhenotype = data.sourceTextPhenotype.toSet.toSeq
            )
          }
        )
      }
    }
  }//end of collectPhenotype
}