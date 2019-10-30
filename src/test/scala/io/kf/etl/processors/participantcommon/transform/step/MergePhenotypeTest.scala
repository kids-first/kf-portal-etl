package io.kf.etl.processors.participantcommon.transform.step

import io.kf.etl.models.dataservice.EPhenotype
import io.kf.etl.models.es.{Participant_ES, Phenotype_ES}
import io.kf.etl.models.ontology.OntologyTerm
import io.kf.etl.processors.participantcommon.transform.step
import io.kf.etl.processors.test.util.EntityUtil.{buildEntityDataSet, buildOntologiesDataSet}
import io.kf.etl.processors.test.util.WithSparkSession
import org.scalatest.{FlatSpec, Matchers}

class MergePhenotypeTest extends FlatSpec with Matchers with WithSparkSession {

  import spark.implicits._

  "process" should "merge phenotypes and participant" in {
    val p1 = Participant_ES(kf_id = Some("participant_id_1"))
    val phenotype_11 = EPhenotype(kfId = Some("phenotype_id_11"), participantId = Some("participant_id_1"), externalId = Some("phenotype 11"))
    val phenotype_12 = EPhenotype(kfId = Some("phenotype_id_12"), participantId = Some("participant_id_1"), externalId = Some("phenotype 12"))

    val p2 = Participant_ES(kf_id = Some("participant_id_2"))
    val phenotype_2 = EPhenotype(kfId = Some("phenotype_id_2"), participantId = Some("participant_id_2"), externalId = Some("phenotype 2"))

    val phenotype_3 = EPhenotype(kfId = Some("phenotype_id_3"), externalId = Some("phenotype 3"))

    val p3 = Participant_ES(kf_id = Some("participant_id_3"))

    val entityDataset = buildEntityDataSet(
      phenotypes = Seq(phenotype_11, phenotype_12, phenotype_2, phenotype_3)
    )
    val result = MergePhenotype(entityDataset, Seq(p1, p2, p3).toDS()).collect()


    result should contain theSameElementsAs Seq(
      Participant_ES(kf_id = Some("participant_id_1"),
        phenotype = Seq(Phenotype_ES(external_id = Some("phenotype 12")), Phenotype_ES(external_id = Some("phenotype 11")))
      ),
      Participant_ES(kf_id = Some("participant_id_2"),
        phenotype = Seq(Phenotype_ES(external_id = Some("phenotype 2")))
      ),

      Participant_ES(kf_id = Some("participant_id_3"))
    )
  }

  it should "merge phenotypes and participant binding all fields for an observed phenotype" in {
    val p1 = Participant_ES(kf_id = Some("participant_id_1"))
    val phenotype_1 = EPhenotype(
      kfId = Some("phenotype_id_1"),
      participantId = Some("participant_id_1"),
      sourceTextPhenotype = Some("phenotype source text 1"),
      observed = Some("positive"),
      createdAt = Some("should be removed"), modifiedAt = Some("should be removed"),
      hpoIdPhenotype = Some("HP:0001166"),
      ageAtEventDays = Some(100),
      snomedIdPhenotype = Some("SNOMED:4"),
      externalId = Some("external id"),
      visible = Some(true)
    )

    val entityDataset = buildEntityDataSet(
      phenotypes = Seq(phenotype_1),
      ontologyData = Some(buildOntologiesDataSet(hpoTerms = Seq(OntologyTerm(id = "HP:0001166", name = "Arachnodactyly"))))
    )

    val result = step.MergePhenotype(entityDataset,Seq(p1).toDS()).collect()

    result should contain theSameElementsAs Seq(
      Participant_ES(kf_id = Some("participant_id_1"),
        phenotype = Seq(Phenotype_ES(
          source_text_phenotype = Some("phenotype source text 1"),
          hpo_phenotype_observed = Some("Arachnodactyly (HP:0001166)"),
          age_at_event_days = Some(100),
          external_id = Some("external id"),
          snomed_phenotype_observed = Some("SNOMED:4"),
          hpo_phenotype_observed_text = Some("Arachnodactyly (HP:0001166)"),
          observed = Some(true)

        ))
      )
    )
  }
  it should "merge phenotypes and participant binding all fields for a not observed phenotype" in {
    val p1 = Participant_ES(kf_id = Some("participant_id_1"))
    val phenotype_1 = EPhenotype(
      kfId = Some("phenotype_id_1"),
      participantId = Some("participant_id_1"),
      sourceTextPhenotype = Some("phenotype source text 1"),
      observed = Some("negative"),
      createdAt = Some("should be removed"), modifiedAt = Some("should be removed"),
      hpoIdPhenotype = Some("HP:0001166")
    )

    val entityDataset = buildEntityDataSet(
      phenotypes = Seq(phenotype_1),
      ontologyData = Some(buildOntologiesDataSet(hpoTerms = Seq(OntologyTerm(id = "HP:0001166", name = "Arachnodactyly"))))
    )

    val result = step.MergePhenotype(entityDataset,Seq(p1).toDS()).collect()

    result should contain theSameElementsAs Seq(
      Participant_ES(kf_id = Some("participant_id_1"),
        phenotype = Seq(Phenotype_ES(
          hpo_phenotype_not_observed = Some("Arachnodactyly (HP:0001166)"),
          observed = Some(false)
        ))
      )
    )
  }

  "transformPhenotypes" should "return a dataset of phenotypes with fields hpoPhenotypeObserved and hpoPhenotypeNotObserved populated from HPO terms" in {

    val entityDataset = buildEntityDataSet(
      phenotypes = Seq(
        EPhenotype(participantId = Some("participant_id_1"), kfId = Some("phenotype_id_1"), observed = Some("negative"), hpoIdPhenotype = Some("HP:0001166"), externalId = Some("1"), sourceTextPhenotype = Some("source")),
        EPhenotype(participantId = Some("participant_id_2"), kfId = Some("phenotype_id_2"), observed = Some("positive"), hpoIdPhenotype = Some("HP:0000924"), externalId = Some("2"), sourceTextPhenotype = Some("source")),
        EPhenotype(participantId = Some("participant_id_1"), kfId = Some("phenotype_id_1"), observed = Some("NEGATIVE"), snomedIdPhenotype = Some("SNOMED:1"), externalId = Some("3"), sourceTextPhenotype = Some("source")),
        EPhenotype(participantId = Some("participant_id_2"), kfId = Some("phenotype_id_2"), observed = Some("POSITIVE"), snomedIdPhenotype = Some("SNOMED:1"), externalId = Some("4"), sourceTextPhenotype = Some("source")),
        EPhenotype(participantId = Some("participant_id_3"), kfId = Some("phenotype_id_3"), observed = Some("unknown"), hpoIdPhenotype = Some("HP:0000924"), externalId = Some("5"), sourceTextPhenotype = Some("source")),
        EPhenotype(participantId = Some("participant_id_4"), kfId = Some("phenotype_id_4"), observed = None, hpoIdPhenotype = Some("HP:0000924"), externalId = Some("6"), sourceTextPhenotype = Some("source")),
        EPhenotype(participantId = Some("participant_id_5"), kfId = Some("phenotype_id_5"), observed = Some("positive"), hpoIdPhenotype = Some("HP:unknown"), externalId = Some("7"), sourceTextPhenotype = Some("source")),
        EPhenotype(participantId = Some("participant_id_6"), kfId = Some("phenotype_id_6"), observed = Some("negative"), hpoIdPhenotype = Some("HP:unknown"), externalId = Some("8"), sourceTextPhenotype = Some("source"))
      ),
      ontologyData = Some(buildOntologiesDataSet(hpoTerms = Seq(
        OntologyTerm(id = "HP:0001166", name = "Arachnodactyly"),
        OntologyTerm(id = "HP:0000924", name = "Abnormality of the skeletal system"),
        OntologyTerm(id = "HP:0099999", name = "Never used")
      )))
    )

    MergePhenotype.transformPhenotypes(entityDataset).collect() should contain theSameElementsAs Seq(
      "participant_id_1" -> Phenotype_ES(hpo_phenotype_not_observed = Some("Arachnodactyly (HP:0001166)"), external_id = Some("1"), observed = Some(false)),
      "participant_id_2" -> Phenotype_ES(hpo_phenotype_observed = Some("Abnormality of the skeletal system (HP:0000924)"), hpo_phenotype_observed_text = Some("Abnormality of the skeletal system (HP:0000924)"), external_id = Some("2"), source_text_phenotype = Some("source"), observed = Some(true)),
      "participant_id_1" -> Phenotype_ES(external_id = Some("3"), snomed_phenotype_not_observed = Some("SNOMED:1"), observed = Some(false)),
      "participant_id_2" -> Phenotype_ES(external_id = Some("4"), snomed_phenotype_observed = Some("SNOMED:1"), source_text_phenotype = Some("source"), observed = Some(true)),
      "participant_id_3" -> Phenotype_ES(external_id = Some("5")),
      "participant_id_4" -> Phenotype_ES(external_id = Some("6")),
      "participant_id_5" -> Phenotype_ES(external_id = Some("7"), observed = Some(true)),
      "participant_id_6" -> Phenotype_ES(external_id = Some("8"), observed = Some(false))
    )
  }


}
