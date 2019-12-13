package io.kf.etl.processors.test.util
import io.kf.etl.models.dataservice.EParticipant
import io.kf.etl.models.es.{Biospecimen_ES, Diagnosis_ES, FamilyComposition_ES, FamilyMember_ES, Family_ES, Outcome_ES, Participant_ES, Phenotype_ES, Study_ES}
import play.api.libs.json._
import play.api.libs.json.Reads._
import play.api.libs.functional.syntax._

object Codecs {


    implicit val readsOutcome_ES: Reads[Outcome_ES] = (
      (JsPath \ "age_at_event_days").readNullable[Int] and
        (JsPath \ "disease_related").readNullable[String] and
        (JsPath \ "kf_id").readNullable[String] and
        (JsPath \ "vital_status").readNullable[String]
      )(Outcome_ES.apply _)

    implicit val readsPhenotype_ES: Reads[Phenotype_ES] = (
      (JsPath \ "age_at_event_days").readNullable[Int] and
        (JsPath \ "ancestral_hpo_ids").readNullable[String] and
        (JsPath \ "external_id").readNullable[String] and
        (JsPath \ "hpo_phenotype_not_observed").readNullable[String] and
        (JsPath \ "hpo_phenotype_observed").readNullable[String] and
        (JsPath \ "hpo_phenotype_observed_text").readNullable[String] and
        (JsPath \ "shared_hpo_ids").readNullable[String] and
        (JsPath \ "snomed_phenotype_not_observed").readNullable[String] and
        (JsPath \ "snomed_phenotype_observed").readNullable[String] and
        (JsPath \ "source_text_phenotype").readNullable[String] and
        (JsPath \ "observed").readNullable[Boolean]
      )(Phenotype_ES.apply _)

    implicit val readsDiagnosis_ES: Reads[Diagnosis_ES] = (
      (JsPath \ "age_at_event_days").readNullable[Int] and
        (JsPath \ "diagnosis_category").readNullable[String] and
        (JsPath \ "external_id").readNullable[String] and
        (JsPath \ "icd_id_diagnosis").readNullable[String] and
        (JsPath \ "kf_id").readNullable[String] and
        (JsPath \ "mondo_id_diagnosis").readNullable[String] and
        (JsPath \ "source_text_diagnosis").readNullable[String] and
        (JsPath \ "uberon_id_tumor_location").readNullable[String] and
        (JsPath \ "source_text_tumor_location").readNullable[String] and
        (JsPath \ "ncit_id_diagnosis").readNullable[String] and
        (JsPath \ "spatial_descriptor").readNullable[String] and
        (JsPath \ "diagnosis").readNullable[String] and
        (JsPath \ "biospecimens").read[Seq[String]]
      )(Diagnosis_ES.apply _)


    implicit val readsFamilyMember_ES: Reads[FamilyMember_ES] = (
      (JsPath \ "alias_group").readNullable[String] and
        (JsPath \ "relationship").readNullable[String] and
        (JsPath \ "consent_type").readNullable[String] and
        (JsPath \ "diagnoses").read[Seq[Diagnosis_ES]] and
        (JsPath \ "ethnicity").readNullable[String] and
        (JsPath \ "external_id").readNullable[String] and
        (JsPath \ "gender").readNullable[String] and
        (JsPath \ "is_proband").readNullable[Boolean] and
        (JsPath \ "kf_id").readNullable[String] and
        (JsPath \ "outcome").readNullable[Outcome_ES] and
        (JsPath \ "phenotype").read[Seq[Phenotype_ES]] and
        (JsPath \ "race").readNullable[String] and
        (JsPath \ "available_data_types").read[Seq[String]]
      )(FamilyMember_ES.apply _)

    implicit val readsFamily_ES: Reads[Family_ES] = (
      (JsPath \ "family_id").readNullable[String] and
        (JsPath \ "family_compositions").read[Seq[FamilyComposition_ES]] and
        (JsPath \ "father_id").readNullable[String] and
        (JsPath \ "mother_id").readNullable[String]
      )(Family_ES.apply _)

    implicit val readsFamilyComposition_ES: Reads[FamilyComposition_ES] = (
      (JsPath \ "composition").readNullable[String] and
        (JsPath \ "shared_hpo_ids").read[Seq[String]] and
        (JsPath \ "available_data_types").read[Seq[String]] and
        (JsPath \ "family_members").read[Seq[FamilyMember_ES]]
      )(FamilyComposition_ES.apply _)

//    implicit val readsBiospecimen_ES: Reads[Biospecimen_ES] = (  //FIXME larger than 22 parameters, should be refactored
//      (JsPath \ "age_at_event_days").readNullable[Int] and
//        (JsPath \ "analyte_type").readNullable[String] and
//        (JsPath \ "composition").readNullable[String] and
//        (JsPath \ "concentration_mg_per_ml").readNullable[Double] and
//        (JsPath \ "consent_type").readNullable[String] and
//        (JsPath \ "dbgap_consent_code").readNullable[String] and
//        (JsPath \ "external_aliquot_id").readNullable[String] and
//        (JsPath \ "external_sample_id").readNullable[String] and
//        (JsPath \ "kf_id").readNullable[String] and
//        (JsPath \ "method_of_sample_procurement").readNullable[String] and
//        (JsPath \ "ncit_id_anatomical_site").readNullable[String] and
//        (JsPath \ "ncit_id_tissue_type").readNullable[String] and
//        (JsPath \ "shipment_date").readNullable[String] and
//        (JsPath \ "shipment_origin").readNullable[String] and
//        (JsPath \ "genomic_files").read[Seq[String]] and
//        (JsPath \ "source_text_tumor_descriptor").readNullable[String] and
//        (JsPath \ "source_text_tissue_type").readNullable[String] and
//        (JsPath \ "source_text_anatomical_site").readNullable[String] and
//        (JsPath \ "spatial_descriptor").readNullable[String] and
//        (JsPath \ "uberon_id_anatomical_site").readNullable[String] and
//        (JsPath \ "volume_ml").readNullable[Double] and
//        (JsPath \ "sequencing_center_id").readNullable[String] and
//        (JsPath \ "diagnoses").read[Seq[Diagnosis_ES]]
//      )(Biospecimen_ES.apply _)

    implicit val readsStudy_ES: Reads[Study_ES] = (
      (JsPath \ "kf_id").readNullable[String] and
        (JsPath \ "attribution").readNullable[String] and
        (JsPath \ "name").readNullable[String] and
        (JsPath \ "version").readNullable[String] and
        (JsPath \ "external_id").readNullable[String] and
        (JsPath \ "release_status").readNullable[String] and
        (JsPath \ "data_access_authority").readNullable[String] and
        (JsPath \ "short_name").readNullable[String]
      )(Study_ES.apply _)


//    implicit val readsParticipants: Reads[Participant_ES] = (
//      (JsPath \ "affected_status").readNullable[Boolean] and
//        (JsPath \ "alias_group").readNullable[String] and
//        (JsPath \ "available_data_types").read[Seq[String]] and
//        (JsPath \ "biospecimens").read[Seq[Biospecimen_ES]] and
//        (JsPath \ "diagnoses").read[Seq[Diagnosis_ES]] and
//        (JsPath \ "diagnosis_category").readNullable[String] and
//        (JsPath \ "ethnicity").readNullable[String] and
//        (JsPath \ "external_id").readNullable[String] and
//        (JsPath \ "family").readNullable[Family_ES] and
//        (JsPath \ "family_id").readNullable[String] and
//        (JsPath \ "gender").readNullable[String] and
//        (JsPath \ "is_proband").readNullable[Boolean] and
//        (JsPath \ "kf_id").readNullable[String] and
//        (JsPath \ "outcome").readNullable[Outcome_ES] and
//        (JsPath \ "phenotype").read[Seq[Phenotype_ES]] and
//        (JsPath \ "race").readNullable[String] and
//        (JsPath \ "study").readNullable[Study_ES]
//      )(Participant_ES.apply _)
}
