package io.kf.etl.models.es

final case class ParticipantCombined_ES (
                                          affectedStatus: Option[Boolean] = None,
                                          aliasGroup: Option[String] = None,
                                          biospecimens: Seq[Biospecimen_ES] = Nil,
                                          diagnoses: Seq[Diagnosis_ES] = Nil,
                                          ethnicity: Option[String] = None,
                                          externalId: Option[String] = None,
                                          family: Option[String] = None,
                                          familyId: Option[String] = None,
                                          gender: Option[String] = None,
                                          isProband: Option[Boolean] = None,
                                          kfId: Option[String] = None,
                                          outcome: Option[Outcome_ES] = None,
                                          phenotype: Seq[Phenotype_ES] = Nil,
                                          race: Option[String] = None,
                                          study: Option[Study_ES] = None,
                                          availableDataTypes: Seq[String] = Nil
                                        )
