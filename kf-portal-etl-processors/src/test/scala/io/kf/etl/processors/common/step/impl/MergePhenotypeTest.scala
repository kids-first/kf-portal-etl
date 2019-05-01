package io.kf.etl.processors.common.step.impl

import io.kf.etl.es.models.{Participant_ES, Phenotype_ES}
import io.kf.etl.external.dataservice.entity.EPhenotype
import io.kf.etl.external.hpo.OntologyTerm
import io.kf.etl.processors.test.util.EntityUtil.{buildEntityDataSet, buildOntologiesDataSet}
import io.kf.etl.processors.test.util.StepContextUtil.buildContext
import io.kf.etl.processors.test.util.WithSparkSession
import org.scalatest.{FlatSpec, Matchers}

class MergePhenotypeTest extends FlatSpec with Matchers with WithSparkSession {

  import spark.implicits._

  "process" should "merge phenotypes and participant" in {
    val p1 = Participant_ES(kfId = Some("participant_id_1"))
    val phenotype_11 = EPhenotype(kfId = Some("phenotype_id_11"), participantId = Some("participant_id_1"), externalId = Some("phenotype 11"))
    val phenotype_13 = EPhenotype(kfId = Some("phenotype_id_13"), participantId = Some("participant_id_1"), externalId = Some("phenotype 11"))
    val phenotype_12 = EPhenotype(kfId = Some("phenotype_id_12"), participantId = Some("participant_id_1"), externalId = Some("phenotype 12"))

    val p2 = Participant_ES(kfId = Some("participant_id_2"))
    val phenotype_2 = EPhenotype(kfId = Some("phenotype_id_2"), participantId = Some("participant_id_2"), externalId = Some("phenotype 2"))

    val phenotype_3 = EPhenotype(kfId = Some("phenotype_id_3"), externalId = Some("phenotype 3"))

    val p3 = Participant_ES(kfId = Some("participant_id_3"))

    val entityDataset = buildEntityDataSet(
      phenotypes = Seq(phenotype_11, phenotype_13, phenotype_12, phenotype_2, phenotype_3)
    )

    val merge = new MergePhenotype(ctx = buildContext(entityDataset))

    val result = merge.process(Seq(p1, p2, p3).toDS()).collect()

    result should contain theSameElementsAs Seq(
      Participant_ES(kfId = Some("participant_id_1"),
        phenotype = Some(Phenotype_ES(externalId = Seq("phenotype 12", "phenotype 11")))
      ),
      Participant_ES(kfId = Some("participant_id_2"),
        phenotype = Some(Phenotype_ES(externalId = Seq("phenotype 2")))
      ),

      Participant_ES(kfId = Some("participant_id_3"))
    )
  }

  it should "merge phenotypes and participant binding all fields for an observed phenotype" in {
    val p1 = Participant_ES(kfId = Some("participant_id_1"))
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

    val merge = new MergePhenotype(ctx = buildContext(entityDataset))

    val result = merge.process(Seq(p1).toDS()).collect()

    result should contain theSameElementsAs Seq(
      Participant_ES(kfId = Some("participant_id_1"),
        phenotype = Some(Phenotype_ES(
          sourceTextPhenotype = Seq("phenotype source text 1"),
          hpoPhenotypeObserved = Seq("Arachnodactyly (HP:0001166)"),
          ageAtEventDays = Seq(100),
          externalId = Seq("external id"),
          snomedPhenotypeObserved = Seq("SNOMED:4"),
          hpoPhenotypeObservedText = Seq("Arachnodactyly (HP:0001166)")

        ))
      )
    )
  }
  it should "merge phenotypes and participant binding all fields for a not observed phenotype" in {
    val p1 = Participant_ES(kfId = Some("participant_id_1"))
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

    val merge = new MergePhenotype(ctx = buildContext(entityDataset))

    val result = merge.process(Seq(p1).toDS()).collect()

    result should contain theSameElementsAs Seq(
      Participant_ES(kfId = Some("participant_id_1"),
        phenotype = Some(Phenotype_ES(
          hpoPhenotypeNotObserved = Seq("Arachnodactyly (HP:0001166)")
        ))
      )
    )
  }

  "joinPhenotypesWithTerms" should "return a dataset of phenotypes with fields hpoPhenotypeObserved and hpoPhenotypeNotObserved populated from HPO terms" in {

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
    val merge = new MergePhenotype(ctx = buildContext(entityDataset))

    merge.transformPhenotypes().collect() should contain theSameElementsAs Seq(
      "participant_id_1" -> Phenotype_ES(hpoPhenotypeNotObserved = Seq("Arachnodactyly (HP:0001166)"), externalId = Seq("1")),
      "participant_id_2" -> Phenotype_ES(hpoPhenotypeObserved = Seq("Abnormality of the skeletal system (HP:0000924)"), hpoPhenotypeObservedText = Seq("Abnormality of the skeletal system (HP:0000924)"), externalId = Seq("2"), sourceTextPhenotype = Seq("source")),
      "participant_id_1" -> Phenotype_ES(externalId = Seq("3"), snomedPhenotypeNotObserved = Seq("SNOMED:1")),
      "participant_id_2" -> Phenotype_ES(externalId = Seq("4"), snomedPhenotypeObserved = Seq("SNOMED:1"), sourceTextPhenotype = Seq("source")),
      "participant_id_3" -> Phenotype_ES(externalId = Seq("5")),
      "participant_id_4" -> Phenotype_ES(externalId = Seq("6")),
      "participant_id_5" -> Phenotype_ES(externalId = Seq("7")),
      "participant_id_6" -> Phenotype_ES(externalId = Seq("8"))
    )
  }


  "collectPhenotype" should "return empty phenotype" in {
    MergePhenotype.reducePhenotype(Nil) shouldBe None
  }

  it should "return a reduced phenotype" in {
    MergePhenotype.reducePhenotype(Seq(
      Phenotype_ES(
        externalId = Seq("1"),
        ageAtEventDays = Seq(10),
        hpoPhenotypeNotObserved = Seq("HPO not observed 1"),
        hpoPhenotypeObserved = Seq("HPO observed 1"),
        hpoPhenotypeObservedText = Seq("HPO observed 1"),
        snomedPhenotypeNotObserved = Seq("SNOMED not observed 1"),
        snomedPhenotypeObserved = Seq("SNOMED observed 1"),
        sourceTextPhenotype = Seq("source text 1")
      ),
      Phenotype_ES(
        externalId = Seq("2"),
        ageAtEventDays = Seq(20),
        hpoPhenotypeNotObserved = Seq("HPO not observed 2"),
        hpoPhenotypeObserved = Seq("HPO observed 2"),
        hpoPhenotypeObservedText = Seq("HPO observed 2"),
        snomedPhenotypeNotObserved = Seq("SNOMED not observed 2"),
        snomedPhenotypeObserved = Seq("SNOMED observed 2"),
        sourceTextPhenotype = Seq("source text 2")
      )
    )) shouldBe Some(Phenotype_ES(
      externalId = Seq("1", "2"),
      ageAtEventDays = Seq(10, 20),
      hpoPhenotypeNotObserved = Seq("HPO not observed 1","HPO not observed 2"),
      hpoPhenotypeObserved = Seq("HPO observed 1","HPO observed 2"),
      hpoPhenotypeObservedText = Seq("HPO observed 1","HPO observed 2"),
      snomedPhenotypeNotObserved = Seq("SNOMED not observed 1","SNOMED not observed 2"),
      snomedPhenotypeObserved = Seq("SNOMED observed 1","SNOMED observed 2"),
      sourceTextPhenotype = Seq("source text 1","source text 2")
    ))
  }


}
