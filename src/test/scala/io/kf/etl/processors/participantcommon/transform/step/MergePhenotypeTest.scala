package io.kf.etl.processors.participantcommon.transform.step

import io.kf.etl.models.dataservice.EPhenotype
import io.kf.etl.models.es.{Participant_ES, OntologicalTermWithParents_ES, Phenotype_ES}
import io.kf.etl.models.ontology.{OntologyTerm, OntologyTermBasic}
import io.kf.etl.processors.common.ProcessorCommonDefinitions.OntologiesDataSet
import io.kf.etl.processors.participantcommon.transform.step
import io.kf.etl.processors.test.util.EntityUtil.buildEntityDataSet
import io.kf.etl.processors.test.util.WithSparkSession
import org.scalatest.{FlatSpec, Matchers}

class MergePhenotypeTest extends FlatSpec with Matchers with WithSparkSession {

  //Ancestors of HP:0009654 | Osteolytic defect of thumb phalanx
  val hpo_0002813: OntologyTermBasic = OntologyTermBasic("HP:0002813", "Abnormality of limb bone morphology")
  val hpo_0009774: OntologyTermBasic = OntologyTermBasic("HP:0009774", "Triangular shaped phalanges of the hand")
  val hpo_0001155: OntologyTermBasic = OntologyTermBasic("HP:0001155", "Abnormality of the hand")
  val hpo_0011297: OntologyTermBasic = OntologyTermBasic("HP:0011297", "Abnormal digit morphology")
  val hpo_0009771: OntologyTermBasic = OntologyTermBasic("HP:0009771", "Osteolytic defects of the phalanges of the hand")
  val hpo_0009699: OntologyTermBasic = OntologyTermBasic("HP:0009699", "Osteolytic defects of the hand bones")
  val hpo_0002817: OntologyTermBasic = OntologyTermBasic("HP:0002817", "Abnormality of the upper limb")
  val hpo_0002797: OntologyTermBasic = OntologyTermBasic("HP:0002797", "Osteolysis")
  val hpo_0003330: OntologyTermBasic = OntologyTermBasic("HP:0003330", "Abnormal bone structure")
  val hpo_0000001: OntologyTermBasic = OntologyTermBasic("HP:0000001", "All")
  val hpo_0005918: OntologyTermBasic = OntologyTermBasic("HP:0005918", "Abnormal finger phalanx morphology")
  val hpo_0000118: OntologyTermBasic = OntologyTermBasic("HP:0000118", "Phenotypic abnormality")
  val hpo_0040068: OntologyTermBasic = OntologyTermBasic("HP:0040068", "Abnormality of limb bone")
  val hpo_0000924: OntologyTermBasic = OntologyTermBasic("HP:0000924", "Abnormality of the skeletal system")
  val hpo_0011844: OntologyTermBasic = OntologyTermBasic("HP:0011844", "Abnormal appendicular skeleton morphology")
  val hpo_0045039: OntologyTermBasic = OntologyTermBasic("HP:0045039", "Osteolysis involving bones of the upper limbs")
  val hpo_0009602: OntologyTermBasic = OntologyTermBasic("HP:0009602", "Abnormality of thumb phalanx")
  val hpo_0040064: OntologyTermBasic = OntologyTermBasic("HP:0040064", "Abnormality of limbs")
  val hpo_0001167: OntologyTermBasic = OntologyTermBasic("HP:0001167", "Abnormality of finger")
  val hpo_0011842: OntologyTermBasic = OntologyTermBasic("HP:0011842", "Abnormality of skeletal morphology")
  val hpo_0001172: OntologyTermBasic = OntologyTermBasic("HP:0001172", "Abnormal thumb morphology")
  val hpo_0040070: OntologyTermBasic = OntologyTermBasic("HP:0040070", "Abnormal upper limb bone morphology")
  val hpo_0002818: OntologyTermBasic = OntologyTermBasic("HP:0002818", "Abnormality of the radius")
  val hpo_0011314: OntologyTermBasic = OntologyTermBasic("HP:0011314", "Abnormality of long bone morphology")
  val hpo_0040073: OntologyTermBasic = OntologyTermBasic("HP:0040073", "Abnormal forearm bone morphology")
  val hpo_0040072: OntologyTermBasic = OntologyTermBasic("HP:0040072", "Abnormality of forearm bone")
  val hpo_0002973: OntologyTermBasic = OntologyTermBasic("HP:0002973", "Abnormality of the forearm")
  val hpo_0001238: OntologyTermBasic = OntologyTermBasic("HP:0001238", "Slender finger")
  val hpo_0100807: OntologyTermBasic = OntologyTermBasic("HP:0100807", "Long fingers")
  val hpo_0009654: OntologyTermBasic = OntologyTermBasic("HP:0009654", "Osteolytic defect of thumb phalanx")
  val hpo_0045009: OntologyTermBasic = OntologyTermBasic("HP:0045009", "Abnormal morphology of the radius")
  val hpo_0001166: OntologyTermBasic = OntologyTermBasic("HP:0001166", "Arachnodactyly")
  val hpo_0001872: OntologyTermBasic = OntologyTermBasic("HP:0001872", "Abnormal thrombocyte morphology")
  val hpo_0001871: OntologyTermBasic = OntologyTermBasic("HP:0001871", "Abnormality of blood and blood-forming tissues")
  val hpo_0011869: OntologyTermBasic = OntologyTermBasic("HP:0011869", "Abnormal platelet function")
  val hpo_0011878: OntologyTermBasic = OntologyTermBasic("HP:0011878", "Abnormal platelet membrane protein expression")
  val hpo_0011879: OntologyTermBasic = OntologyTermBasic("HP:0011879", "Decreased platelet glycoprotein Ib-IX-V")

  val hpo_0000175: OntologyTermBasic = OntologyTermBasic("HP:0000175", "Cleft palate")
  val hpo_0000202: OntologyTermBasic = OntologyTermBasic("HP:0000202", "Oral cleft")
  val hpo_0100737: OntologyTermBasic = OntologyTermBasic("HP:0100737", "Abnormal hard palate morphology")
  val hpo_0000163: OntologyTermBasic = OntologyTermBasic("HP:0000163", "Abnormal oral cavity morphology")
  val hpo_0031816: OntologyTermBasic = OntologyTermBasic("HP:0031816", "Abnormal oral morphology")
  val hpo_0000153: OntologyTermBasic = OntologyTermBasic("HP:0000153", "Abnormality of the mouth")
  val hpo_0000271: OntologyTermBasic = OntologyTermBasic("HP:0000271", "Abnormality of the face")
  val hpo_0000234: OntologyTermBasic = OntologyTermBasic("HP:0000234", "Abnormality of the head")
  val hpo_0000152: OntologyTermBasic = OntologyTermBasic("HP:0000152", "Abnormality of head or neck")
  val hpo_0000174: OntologyTermBasic = OntologyTermBasic("HP:0000174", "Abnormal palate morphology")

  import spark.implicits._

  val ontologiesDataSet: OntologiesDataSet = OntologiesDataSet(
    hpoTerms = spark.read.json("../kf-portal-etl/kf-portal-etl-docker/hpo_terms.json.gz").select("id", "name", "parents", "ancestors", "is_leaf").as[OntologyTerm],
    mondoTerms = Seq.empty[OntologyTerm].toDS(),
    ncitTerms = Seq.empty[OntologyTermBasic].toDS()
  )

  "process" should "merge phenotypes and participant" in {
    val p1 = Participant_ES(kf_id = Some("participant_id_1"))
    val phenotype_11 = EPhenotype(kfId = Some("phenotype_id_11"), ageAtEventDays = Some(15), participantId = Some("participant_id_1"), externalId = Some("phenotype 11"), observed = Some("positive"))
    val phenotype_13 = EPhenotype(kfId = Some("phenotype_id_13"), ageAtEventDays = Some(18), participantId = Some("participant_id_1"), externalId = Some("phenotype 13"), observed = Some("positive"))
    val phenotype_12 = EPhenotype(kfId = Some("phenotype_id_12"), participantId = Some("participant_id_1"), externalId = Some("phenotype 12"), observed = Some("negative"))
    val phenotype_14 = EPhenotype(kfId = Some("phenotype_id_14"), ageAtEventDays = Some(22), participantId = Some("participant_id_1"), externalId = Some("phenotype 14"), observed = None)
    val phenotype_2 = EPhenotype(kfId = Some("phenotype_id_2"), participantId = Some("participant_id_2"), externalId = Some("phenotype 2"), observed = Some("positive"))
    val phenotype_3 = EPhenotype(kfId = Some("phenotype_id_3"), externalId = Some("phenotype 3"), observed = Some("positive"))

    val p2 = Participant_ES(kf_id = Some("participant_id_2"))

    val p3 = Participant_ES(kf_id = Some("participant_id_3"))

    val entityDataset = buildEntityDataSet(
      phenotypes = Seq(phenotype_11, phenotype_12, phenotype_13, phenotype_2, phenotype_3, phenotype_14),
      ontologyData = Some(ontologiesDataSet)
    )
    val result = MergePhenotype(entityDataset, Seq(p1, p2, p3).toDS()).collect()

    result.sortBy(_.kf_id) should contain theSameElementsAs Seq(
      Participant_ES(kf_id = Some("participant_id_1"),
        phenotype = Seq(
          Phenotype_ES(
            external_id = Some("phenotype 11"),
            age_at_event_days = Some(15),
            observed = Some(true)),
          Phenotype_ES(
            external_id = Some("phenotype 13"),
            age_at_event_days = Some(18),
            observed = Some(true)),
          Phenotype_ES(
            external_id = Some("phenotype 12"),
            observed = Some(false))
          //  phenotype 14 - not observed
        )
      ),
      Participant_ES(kf_id = Some("participant_id_2"),
        phenotype = Seq(Phenotype_ES(external_id = Some("phenotype 2"), observed = Some(true)))
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
      ontologyData = Some(ontologiesDataSet)
    )

    val result = step.MergePhenotype(entityDataset,Seq(p1).toDS()).collect()
    val resultSorted = result.map(p =>
      p.copy(observed_phenotypes = p.observed_phenotypes.sorted)
    )

    resultSorted should contain theSameElementsAs Seq(
      Participant_ES(kf_id = Some("participant_id_1"),
        phenotype = Seq(
          Phenotype_ES(
            source_text_phenotype = Some("phenotype source text 1"),
            hpo_phenotype_observed = Some("Arachnodactyly (HP:0001166)"),
            age_at_event_days = Some(100),
            external_id = Some("external id"),
            snomed_phenotype_observed = Some("SNOMED:4"),
            hpo_phenotype_observed_text = Some("Arachnodactyly (HP:0001166)"),
            observed = Some(true)
          )),
        observed_phenotypes = Seq(
          OntologicalTermWithParents_ES(name = hpo_0001167.toString, parents = Seq(hpo_0001155.toString, hpo_0011297.toString), age_at_event_days = Set(100)),
          OntologicalTermWithParents_ES(name = hpo_0040064.toString, parents = Seq(hpo_0000118.toString), age_at_event_days = Set(100)),
          OntologicalTermWithParents_ES(name = hpo_0011842.toString, parents = Seq(hpo_0000924.toString), age_at_event_days = Set(100)),
          OntologicalTermWithParents_ES(name = hpo_0001238.toString, parents = Seq(hpo_0001167.toString), age_at_event_days = Set(100)),
          OntologicalTermWithParents_ES(name = hpo_0002817.toString, parents = Seq(hpo_0040064.toString), age_at_event_days = Set(100)),
          OntologicalTermWithParents_ES(name = hpo_0001155.toString, parents = Seq(hpo_0002817.toString), age_at_event_days = Set(100)),
          OntologicalTermWithParents_ES(name = hpo_0011844.toString, parents = Seq(hpo_0011842.toString), age_at_event_days = Set(100)),
          OntologicalTermWithParents_ES(name = hpo_0000924.toString, parents = Seq(hpo_0000118.toString), age_at_event_days = Set(100)),
          OntologicalTermWithParents_ES(name = hpo_0100807.toString, parents = Seq(hpo_0001167.toString), age_at_event_days = Set(100)),
          OntologicalTermWithParents_ES(name = hpo_0000118.toString, parents = Seq(hpo_0000001.toString), age_at_event_days = Set(100)),
          OntologicalTermWithParents_ES(name = hpo_0001166.toString, parents = Seq(hpo_0001238.toString, hpo_0100807.toString), age_at_event_days = Set(100), is_leaf = true),
          OntologicalTermWithParents_ES(name = hpo_0000001.toString, parents = Seq.empty[String], age_at_event_days = Set(100)),
          OntologicalTermWithParents_ES(name = hpo_0011297.toString, parents = Seq(hpo_0002813.toString), age_at_event_days = Set(100)),
          OntologicalTermWithParents_ES(name = hpo_0040068.toString, parents = Seq(hpo_0000924.toString, hpo_0040064.toString), age_at_event_days = Set(100)),
          OntologicalTermWithParents_ES(name = hpo_0002813.toString, parents = Seq(hpo_0011844.toString, hpo_0040068.toString), age_at_event_days = Set(100))
        ).sorted
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
      ontologyData = Some(ontologiesDataSet)
    )

    val result = step.MergePhenotype(entityDataset,Seq(p1).toDS()).collect()

    result should contain theSameElementsAs Seq(
      Participant_ES(kf_id = Some("participant_id_1"),
        phenotype = Seq(Phenotype_ES(
          hpo_phenotype_not_observed = Some(hpo_0001166.toString),
          observed = Some(false)
        )),
        non_observed_phenotypes = Seq(
          OntologicalTermWithParents_ES(
            name = hpo_0001166.toString,
            parents = Seq(hpo_0001238.toString, hpo_0100807.toString), // FIXME sorted
            age_at_event_days = Set.empty[Int],
            is_leaf = true
          )
        )
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
      ontologyData = Some(ontologiesDataSet)
    )

    MergePhenotype.transformPhenotypes(entityDataset).collect().sortBy(_._1) should contain theSameElementsAs Seq(
      ("participant_id_1",
        Phenotype_ES(
          hpo_phenotype_not_observed = Some(hpo_0001166.toString),
          external_id = Some("1"),
          observed = Some(false)
        ),
        Seq(OntologicalTermWithParents_ES(hpo_0001166.toString, Seq(hpo_0001238.toString, hpo_0100807.toString), is_leaf = true))),
      ("participant_id_1", Phenotype_ES(external_id = Some("3"), snomed_phenotype_not_observed = Some("SNOMED:1"), observed = Some(false)), Nil),
      ("participant_id_2",
        Phenotype_ES(
          hpo_phenotype_observed = Some(hpo_0000924.toString),
          hpo_phenotype_observed_text = Some(hpo_0000924.toString),
          external_id = Some("2"),
          source_text_phenotype = Some("source"),
          observed = Some(true)),
        Seq(OntologicalTermWithParents_ES(hpo_0000924.toString, Seq(hpo_0000118.toString), is_leaf = false))
      ),
      ("participant_id_2", Phenotype_ES(external_id = Some("4"), snomed_phenotype_observed = Some("SNOMED:1"), source_text_phenotype = Some("source"), observed = Some(true)), Nil),
//      ("participant_id_3", Phenotype_ES(external_id = Some("5")), Nil), observed is None
//      ("participant_id_4", Phenotype_ES(external_id = Some("6")), Nil), observed is None
      ("participant_id_5", Phenotype_ES(external_id = Some("7"), observed = Some(true)), Nil),
      ("participant_id_6", Phenotype_ES(external_id = Some("8"), observed = Some(false)), Nil)
    )
  }

  "process" should "merge phenotypes and their ancestors" in {
    val p1 = Participant_ES(kf_id = Some("participant_id_1"))
    val phenotype_11 = EPhenotype(kfId = Some("phenotype_id_11"), ageAtEventDays = Some(15), hpoIdPhenotype = Some("HP:0009654"), observed = Some("positive"), participantId = Some("participant_id_1"), externalId = Some("phenotype 11"))
    val phenotype_16 = EPhenotype(kfId = Some("phenotype_id_11"), ageAtEventDays = None, hpoIdPhenotype = Some("HP:0000175"), observed = Some("positive"), participantId = Some("participant_id_1"), externalId = Some("phenotype 11"))
    val phenotype_13 = EPhenotype(kfId = Some("phenotype_id_13"), ageAtEventDays = Some(18), hpoIdPhenotype = Some("HP:0045009"), observed = Some("positive"), participantId = Some("participant_id_1"), externalId = Some("phenotype 13"))
    val phenotype_14 = EPhenotype(kfId = Some("phenotype_id_14"), ageAtEventDays = Some(9999), hpoIdPhenotype = Some("HP:0031816"), participantId = Some("participant_id_1"), externalId = Some("phenotype 14")) // observed in None
    val phenotype_15 = EPhenotype(kfId = Some("phenotype_id_15"), ageAtEventDays = Some(22), hpoIdPhenotype = Some("HP:0011879"), observed = Some("negative"), participantId = Some("participant_id_1"), externalId = Some("phenotype 15")) // Not observed
    val phenotype_12 = EPhenotype(kfId = Some("phenotype_id_12"), participantId = Some("participant_id_1"), externalId = Some("phenotype 12")) // observed in None

    val p2 = Participant_ES(kf_id = Some("participant_id_2"))
    val phenotype_2 = EPhenotype(kfId = Some("phenotype_id_2"), participantId = Some("participant_id_2"), externalId = Some("phenotype 2"))

    val phenotype_3 = EPhenotype(kfId = Some("phenotype_id_3"), externalId = Some("phenotype 3"))

    val p3 = Participant_ES(kf_id = Some("participant_id_3"))

    val entityDataset = buildEntityDataSet(
      phenotypes = Seq(phenotype_11, phenotype_12, phenotype_13, phenotype_14, phenotype_15, phenotype_16, phenotype_2, phenotype_3),
      ontologyData = Some(ontologiesDataSet)
    )
    val result = MergePhenotype(entityDataset, Seq(p1, p2, p3).toDS()).collect()


    result.map(_.kf_id) should contain theSameElementsAs Seq(
      Some("participant_id_1"), Some("participant_id_2"), Some("participant_id_3")
    )

    (result.find(_.kf_id.contains("participant_id_3")) match {
      case Some(a) => a.observed_phenotypes
      case None => Nil
    }) should contain theSameElementsAs Seq.empty[OntologicalTermWithParents_ES]

    (result.find(_.kf_id.contains("participant_id_2")) match {
      case Some(a) => a.observed_phenotypes
      case None => Nil
    }) should contain theSameElementsAs Seq.empty[OntologicalTermWithParents_ES]

    (result.find(_.kf_id.contains("participant_id_1")) match {
      case Some(a) => a.observed_phenotypes
      case None => Nil
    }) should contain theSameElementsAs Seq(
//      No age_at_event_days (but observed)
      OntologicalTermWithParents_ES(name = hpo_0000175.toString, parents = Seq(hpo_0000202.toString, hpo_0100737.toString), age_at_event_days = Set.empty[Int]),
      OntologicalTermWithParents_ES(name = hpo_0000202.toString, parents = Seq(hpo_0000163.toString), age_at_event_days = Set.empty[Int]),
      OntologicalTermWithParents_ES(name = hpo_0000163.toString, parents = Seq(hpo_0031816.toString), age_at_event_days = Set.empty[Int]),
      OntologicalTermWithParents_ES(name = hpo_0031816.toString, parents = Seq(hpo_0000153.toString), age_at_event_days = Set.empty[Int]),
      OntologicalTermWithParents_ES(name = hpo_0000153.toString, parents = Seq(hpo_0000271.toString), age_at_event_days = Set.empty[Int]),
      OntologicalTermWithParents_ES(name = hpo_0000271.toString, parents = Seq(hpo_0000234.toString), age_at_event_days = Set.empty[Int]),
      OntologicalTermWithParents_ES(name = hpo_0000234.toString, parents = Seq(hpo_0000152.toString), age_at_event_days = Set.empty[Int]),
      OntologicalTermWithParents_ES(name = hpo_0000152.toString, parents = Seq(hpo_0000118.toString), age_at_event_days = Set.empty[Int]),
      OntologicalTermWithParents_ES(name = hpo_0100737.toString, parents = Seq(hpo_0000174.toString), age_at_event_days = Set.empty[Int]),
      OntologicalTermWithParents_ES(name = hpo_0000174.toString, parents = Seq(hpo_0000163.toString), age_at_event_days = Set.empty[Int]),
//      OntologicalTermWithParents_ES(name = hpo_0000118.toString, parents = Seq(hpo_0000001.toString), age_at_event_days = Set.empty[Int]),
//      OntologicalTermWithParents_ES(name = hpo_0000001.toString, parents = Seq.empty[String], age_at_event_days = Set.empty[Int]),

      //15 only
      OntologicalTermWithParents_ES(name = hpo_0009654.toString, parents = Seq(hpo_0009602.toString, hpo_0009771.toString), age_at_event_days = Set(15)),
      OntologicalTermWithParents_ES(name = hpo_0009602.toString, parents = Seq(hpo_0001172.toString, hpo_0009774.toString), age_at_event_days = Set(15)),
      OntologicalTermWithParents_ES(name = hpo_0009771.toString, parents = Seq(hpo_0005918.toString, hpo_0009699.toString), age_at_event_days = Set(15)),
      OntologicalTermWithParents_ES(name = hpo_0001172.toString, parents = Seq(hpo_0001167.toString), age_at_event_days = Set(15)),
      OntologicalTermWithParents_ES(name = hpo_0009774.toString, parents = Seq(hpo_0005918.toString), age_at_event_days = Set(15)),
      OntologicalTermWithParents_ES(name = hpo_0009699.toString, parents = Seq(hpo_0001155.toString, hpo_0045039.toString), age_at_event_days = Set(15)),
      OntologicalTermWithParents_ES(name = hpo_0045039.toString, parents = Seq(hpo_0002797.toString, hpo_0040070.toString), age_at_event_days = Set(15)),
      OntologicalTermWithParents_ES(name = hpo_0001167.toString, parents = Seq(hpo_0001155.toString, hpo_0011297.toString), age_at_event_days = Set(15)),
      OntologicalTermWithParents_ES(name = hpo_0005918.toString, parents = Seq(hpo_0001167.toString), age_at_event_days = Set(15)),
      OntologicalTermWithParents_ES(name = hpo_0001155.toString, parents = Seq(hpo_0002817.toString), age_at_event_days = Set(15)),
      OntologicalTermWithParents_ES(name = hpo_0011297.toString, parents = Seq(hpo_0002813.toString), age_at_event_days = Set(15)),
      OntologicalTermWithParents_ES(name = hpo_0003330.toString, parents = Seq(hpo_0011842.toString), age_at_event_days = Set(15)),
      OntologicalTermWithParents_ES(name = hpo_0002797.toString, parents = Seq(hpo_0003330.toString), age_at_event_days = Set(15)),

      //18 only
      OntologicalTermWithParents_ES(name = hpo_0045009.toString, parents = Seq(hpo_0002818.toString, hpo_0011314.toString, hpo_0040073.toString), age_at_event_days = Set(18)),
      OntologicalTermWithParents_ES(name = hpo_0002818.toString, parents = Seq(hpo_0040072.toString), age_at_event_days = Set(18)),
      OntologicalTermWithParents_ES(name = hpo_0011314.toString, parents = Seq(hpo_0011844.toString), age_at_event_days = Set(18)),
      OntologicalTermWithParents_ES(name = hpo_0040073.toString, parents = Seq(hpo_0040072.toString), age_at_event_days = Set(18)),
      OntologicalTermWithParents_ES(name = hpo_0040072.toString, parents = Seq(hpo_0002973.toString, hpo_0040070.toString), age_at_event_days = Set(18)),
      OntologicalTermWithParents_ES(name = hpo_0002973.toString, parents = Seq(hpo_0002817.toString), age_at_event_days = Set(18)),

      //15 & 18
      OntologicalTermWithParents_ES(name = hpo_0040070.toString, parents = Seq(hpo_0002813.toString, hpo_0002817.toString), age_at_event_days = Set(15, 18)),
      OntologicalTermWithParents_ES(name = hpo_0002817.toString, parents = Seq(hpo_0040064.toString), age_at_event_days = Set(15, 18)),
      OntologicalTermWithParents_ES(name = hpo_0002813.toString, parents = Seq(hpo_0011844.toString, hpo_0040068.toString), age_at_event_days = Set(15, 18)),
      OntologicalTermWithParents_ES(name = hpo_0011844.toString, parents = Seq(hpo_0011842.toString), age_at_event_days = Set(15, 18)),
      OntologicalTermWithParents_ES(name = hpo_0040064.toString, parents = Seq(hpo_0000118.toString), age_at_event_days = Set(15, 18)),
      OntologicalTermWithParents_ES(name = hpo_0040068.toString, parents = Seq(hpo_0000924.toString, hpo_0040064.toString), age_at_event_days = Set(15, 18)),
      OntologicalTermWithParents_ES(name = hpo_0011842.toString, parents = Seq(hpo_0000924.toString), age_at_event_days = Set(15, 18)),
      OntologicalTermWithParents_ES(name = hpo_0000924.toString, parents = Seq(hpo_0000118.toString), age_at_event_days = Set(15, 18)),
      OntologicalTermWithParents_ES(name = hpo_0000118.toString, parents = Seq(hpo_0000001.toString), age_at_event_days = Set(15, 18)),
      OntologicalTermWithParents_ES(name = hpo_0000001.toString, parents = Seq.empty[String], age_at_event_days = Set(15, 18))
    )

    (result.find(_.kf_id.contains("participant_id_1")) match {
      case Some(a) => a.non_observed_phenotypes
      case None => Nil
    }) should contain theSameElementsAs Seq(
      //22 only
      OntologicalTermWithParents_ES(name = hpo_0011879.toString, parents = Seq(hpo_0011878.toString), age_at_event_days = Set(22), is_leaf = true),
      OntologicalTermWithParents_ES(name = hpo_0011878.toString, parents = Seq(hpo_0011869.toString), age_at_event_days = Set(22)),
      OntologicalTermWithParents_ES(name = hpo_0011869.toString, parents = Seq(hpo_0001872.toString), age_at_event_days = Set(22)),
      OntologicalTermWithParents_ES(name = hpo_0001872.toString, parents = Seq(hpo_0001871.toString), age_at_event_days = Set(22)),
      OntologicalTermWithParents_ES(name = hpo_0001871.toString, parents = Seq(hpo_0000118.toString), age_at_event_days = Set(22)),
      OntologicalTermWithParents_ES(name = hpo_0000118.toString, parents = Seq(hpo_0000001.toString), age_at_event_days = Set(22)),
      OntologicalTermWithParents_ES(name = hpo_0000001.toString, parents = Seq.empty[String], age_at_event_days = Set(22))
    )
  }
}
