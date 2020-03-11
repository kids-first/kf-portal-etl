package io.kf.etl.models.es

final case class PhenotypeWithParents_ES(
                                          name: String,
                                          parents: Seq[String] = Nil,
                                          age_at_event_days: Seq[Int] = Nil
                                        )
