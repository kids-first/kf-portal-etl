package io.kf.etl.processor.document.transform.steps

import io.kf.etl.dbschema.{TGraphPath, TPhenotype}
import io.kf.etl.model.{HPO, Participant, Phenotype}
import io.kf.etl.processor.common.ProcessorCommonDefinitions.HPOReference
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Dataset

import scala.collection.mutable.ListBuffer

class MergePhenotype(override val ctx:StepContext) extends StepExecutable[Dataset[Participant], Dataset[Participant]] {
  override def process(participants: Dataset[Participant]): Dataset[Participant] = {

    import ctx.parentContext.sparkSession.implicits._

    participants.joinWith(ctx.dbTables.phenotype, participants.col("kfId") === ctx.dbTables.phenotype.col("participantId"), "left").groupByKey(_._1.kfId).mapGroups((parId, iterator) => {
      val list = iterator.toList
      list(0)._1.copy(
        phenotype = collectPhenotype(
          list.collect{
            case tuple if(tuple._2 != null) => tuple._2
          }
        )
      )
    })
  }

  private def generateHpoRefs(): Broadcast[Map[String, Seq[String]]] = {
    import ctx.parentContext.sparkSession.implicits._
    ctx.parentContext.sparkSession.sparkContext.broadcast(

      ctx.dbTables.graphPath.groupByKey(_.term1).mapGroups((term, iterator) => {
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

  private def collectPhenotype(pheotypes: Seq[TPhenotype]): Option[Phenotype] = {

    val hpoRefs = generateHpoRefs()

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
              }).toSet.toSeq

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


}