package io.kf.etl.processors.participantcommon.transform.step

import io.kf.etl.models.dataservice.EPhenotype
import io.kf.etl.models.es.{OntologicalTermWithParents_ES, Participant_ES, Phenotype_ES}
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
    val phenotype_11 = EPhenotype(kf_id = Some("phenotype_id_11"), age_at_event_days = Some(15), participant_id = Some("participant_id_1"), external_id = Some("phenotype 11"), observed = Some("positive"))
    val phenotype_13 = EPhenotype(kf_id = Some("phenotype_id_13"), age_at_event_days = Some(18), participant_id = Some("participant_id_1"), external_id = Some("phenotype 13"), observed = Some("positive"))
    val phenotype_12 = EPhenotype(kf_id = Some("phenotype_id_12"), participant_id = Some("participant_id_1"), external_id = Some("phenotype 12"), observed = Some("negative"))
    val phenotype_14 = EPhenotype(kf_id = Some("phenotype_id_14"), age_at_event_days = Some(22), participant_id = Some("participant_id_1"), external_id = Some("phenotype 14"), observed = None)
    val phenotype_2 = EPhenotype(kf_id = Some("phenotype_id_2"), participant_id = Some("participant_id_2"), external_id = Some("phenotype 2"), observed = Some("positive"))
    val phenotype_3 = EPhenotype(kf_id = Some("phenotype_id_3"), external_id = Some("phenotype 3"), observed = Some("positive"))

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
      kf_id = Some("phenotype_id_1"),
      participant_id = Some("participant_id_1"),
      source_text_phenotype = Some("phenotype source text 1"),
      observed = Some("positive"),
      created_at = Some("should be removed"), modified_at = Some("should be removed"),
      hpo_id_phenotype = Some("HP:0001166"),
      age_at_event_days = Some(100),
      snomed_id_phenotype = Some("SNOMED:4"),
      external_id = Some("external id"),
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
          OntologicalTermWithParents_ES(name = hpo_0001166.toString, parents = Seq(hpo_0001238.toString, hpo_0100807.toString), age_at_event_days = Set(100), is_leaf = true, is_tagged = true),
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
      kf_id = Some("phenotype_id_1"),
      participant_id = Some("participant_id_1"),
      source_text_phenotype = Some("phenotype source text 1"),
      observed = Some("negative"),
      created_at = Some("should be removed"), modified_at = Some("should be removed"),
      hpo_id_phenotype = Some("HP:0001166")
    )

    val entityDataset = buildEntityDataSet(
      phenotypes = Seq(phenotype_1),
      ontologyData = Some(ontologiesDataSet)
    )

    val result = step.MergePhenotype(entityDataset,Seq(p1).toDS()).collect()
    val resultSorted = result.map(p =>
      p.copy(non_observed_phenotypes = p.non_observed_phenotypes.sorted)
    )

    resultSorted should contain theSameElementsAs Seq(
      Participant_ES(kf_id = Some("participant_id_1"),
        phenotype = Seq(Phenotype_ES(
          hpo_phenotype_not_observed = Some(hpo_0001166.toString),
          observed = Some(false)
        )),
        non_observed_phenotypes = Seq(
          OntologicalTermWithParents_ES(name = hpo_0001166.toString, parents = Seq(hpo_0001238.toString, hpo_0100807.toString), age_at_event_days = Set.empty[Int], is_leaf = true, is_tagged = true),
          OntologicalTermWithParents_ES(name = hpo_0100807.toString, parents = Seq(hpo_0001167.toString), age_at_event_days = Set.empty[Int]),
          OntologicalTermWithParents_ES(name = hpo_0001238.toString, parents = Seq(hpo_0001167.toString), age_at_event_days = Set.empty[Int]),
          OntologicalTermWithParents_ES(name = hpo_0001167.toString, parents = Seq(hpo_0001155.toString, hpo_0011297.toString), age_at_event_days = Set.empty[Int]),
          OntologicalTermWithParents_ES(name = hpo_0011297.toString, parents = Seq(hpo_0002813.toString), age_at_event_days = Set.empty[Int]),
          OntologicalTermWithParents_ES(name = hpo_0002813.toString, parents = Seq(hpo_0011844.toString, hpo_0040068.toString), age_at_event_days = Set.empty[Int]),
          OntologicalTermWithParents_ES(name = hpo_0040068.toString, parents = Seq(hpo_0000924.toString, hpo_0040064.toString), age_at_event_days = Set.empty[Int]),

          OntologicalTermWithParents_ES(name = hpo_0011844.toString, parents = Seq(hpo_0011842.toString), age_at_event_days = Set.empty[Int]),
          OntologicalTermWithParents_ES(name = hpo_0011842.toString, parents = Seq(hpo_0000924.toString), age_at_event_days = Set.empty[Int]),
          OntologicalTermWithParents_ES(name = hpo_0000924.toString, parents = Seq(hpo_0000118.toString), age_at_event_days = Set.empty[Int]),

          OntologicalTermWithParents_ES(name = hpo_0001155.toString, parents = Seq(hpo_0002817.toString), age_at_event_days = Set.empty[Int]),
          OntologicalTermWithParents_ES(name = hpo_0002817.toString, parents = Seq(hpo_0040064.toString), age_at_event_days = Set.empty[Int]),
          OntologicalTermWithParents_ES(name = hpo_0040064.toString, parents = Seq(hpo_0000118.toString), age_at_event_days = Set.empty[Int]),
          OntologicalTermWithParents_ES(name = hpo_0000118.toString, parents = Seq(hpo_0000001.toString), age_at_event_days = Set.empty[Int]),
          OntologicalTermWithParents_ES(name = hpo_0000001.toString, parents = Seq.empty[String], age_at_event_days = Set.empty[Int])
        ).sorted
      )
    )
  }

  "transformPhenotypes" should "return a dataset of phenotypes with fields hpoPhenotypeObserved and hpoPhenotypeNotObserved populated from HPO terms" in {

    val entityDataset = buildEntityDataSet(
      phenotypes = Seq(
        EPhenotype(participant_id = Some("participant_id_1"), kf_id = Some("phenotype_id_1"), observed = Some("negative"), hpo_id_phenotype = Some("HP:0001166"), external_id = Some("1"), source_text_phenotype = Some("source")),
        EPhenotype(participant_id = Some("participant_id_2"), kf_id = Some("phenotype_id_2"), observed = Some("positive"), hpo_id_phenotype = Some("HP:0000924"), external_id = Some("2"), source_text_phenotype = Some("source")),
        EPhenotype(participant_id = Some("participant_id_1"), kf_id = Some("phenotype_id_1"), observed = Some("NEGATIVE"), snomed_id_phenotype = Some("SNOMED:1"), external_id = Some("3"), source_text_phenotype = Some("source")),
        EPhenotype(participant_id = Some("participant_id_2"), kf_id = Some("phenotype_id_2"), observed = Some("POSITIVE"), snomed_id_phenotype = Some("SNOMED:1"), external_id = Some("4"), source_text_phenotype = Some("source")),
        EPhenotype(participant_id = Some("participant_id_3"), kf_id = Some("phenotype_id_3"), observed = Some("unknown"), hpo_id_phenotype = Some("HP:0000924"), external_id = Some("5"), source_text_phenotype = Some("source")),
        EPhenotype(participant_id = Some("participant_id_4"), kf_id = Some("phenotype_id_4"), observed = None, hpo_id_phenotype = Some("HP:0000924"), external_id = Some("6"), source_text_phenotype = Some("source")),
        EPhenotype(participant_id = Some("participant_id_5"), kf_id = Some("phenotype_id_5"), observed = Some("positive"), hpo_id_phenotype = Some("HP:unknown"), external_id = Some("7"), source_text_phenotype = Some("source")),
        EPhenotype(participant_id = Some("participant_id_6"), kf_id = Some("phenotype_id_6"), observed = Some("negative"), hpo_id_phenotype = Some("HP:unknown"), external_id = Some("8"), source_text_phenotype = Some("source"))
      ),
      ontologyData = Some(ontologiesDataSet)
    )

    MergePhenotype
      .transformPhenotypes(entityDataset)
      .collect()
      .sortBy(_._1)
      .map{ case(participantId, phentype, _) => (participantId, phentype) } should contain theSameElementsAs Seq(
      ("participant_id_1",
        Phenotype_ES(
          hpo_phenotype_not_observed = Some(hpo_0001166.toString),
          external_id = Some("1"),
          observed = Some(false)
        )),
      ("participant_id_1", Phenotype_ES(external_id = Some("3"), snomed_phenotype_not_observed = Some("SNOMED:1"), observed = Some(false))),
      ("participant_id_2",
        Phenotype_ES(
          hpo_phenotype_observed = Some(hpo_0000924.toString),
          hpo_phenotype_observed_text = Some(hpo_0000924.toString),
          external_id = Some("2"),
          source_text_phenotype = Some("source"),
          observed = Some(true))
      ),
      ("participant_id_2", Phenotype_ES(external_id = Some("4"), snomed_phenotype_observed = Some("SNOMED:1"), source_text_phenotype = Some("source"), observed = Some(true))),
//      ("participant_id_3", Phenotype_ES(external_id = Some("5")), Nil), observed is None
//      ("participant_id_4", Phenotype_ES(external_id = Some("6")), Nil), observed is None
      ("participant_id_5", Phenotype_ES(external_id = Some("7"), observed = Some(true))),
      ("participant_id_6", Phenotype_ES(external_id = Some("8"), observed = Some(false)))
    )

    //Array(
    // (participant_id_1,Phenotype_ES(None,None,Some(3),None,None,None,None,Some(SNOMED:1),None,None,Some(false)),List()),
    // (participant_id_1,Phenotype_ES(None,None,Some(1),Some(Arachnodactyly (HP:0001166)),None,None,None,None,None,None,Some(false)),List(OntologicalTermWithParents_ES(Arachnodactyly (HP:0001166),List(Slender finger (HP:0001238), Long fingers (HP:0100807)),Set(),true), OntologicalTermWithParents_ES(Abnormality of the skeletal system (HP:0000924),List(Phenotypic abnormality (HP:0000118)),Set(),false), OntologicalTermWithParents_ES(Abnormality of the upper limb (HP:0002817),List(Abnormality of limbs (HP:0040064)),Set(),false), OntologicalTermWithParents_ES(Long fingers (HP:0100807),List(Abnormality of finger (HP:0001167)),Set(),false), OntologicalTermWithParents_ES(All (HP:0000001),List(),Set(),false), OntologicalTermWithParents_ES(Abnormality of the hand (HP:0001155),List(Abnormality of the upper limb (HP:0002817)),Set(),false), OntologicalTermWithParents_ES(Abnormality of finger (HP:0001167),List(Abnormality of the hand (HP:0001155), Abnormal digit morphology (HP:0011297)),Set(),false), OntologicalTermWithParents_ES(Abnormality of limbs (HP:0040064),List(Phenotypic abnormality (HP:0000118)),Set(),false), OntologicalTermWithParents_ES(Abnormality of limb bone (HP:0040068),List(Abnormality of the skeletal system (HP:0000924), Abnormality of limbs (HP:0040064)),Set(),false), OntologicalTermWithParents_ES(Phenotypic abnormality (HP:0000118),List(All (HP:0000001)),Set(),false), OntologicalTermWithParents_ES(Abnormal appendicular skeleton morphology (HP:0011844),List(Abnormality of skeletal morphology (HP:0011842)),Set(),false), OntologicalTermWithParents_ES(Slender finger (HP:0001238),List(Abnormality of finger (HP:0001167)),Set(),false), OntologicalTermWithParents_ES(Abnormal digit morphology (HP:0011297),List(Abnormality of limb bone morphology (HP:0002813)),Set(),false), OntologicalTermWithParents_ES(Abnormality of skeletal morphology (HP:0011842),List(Abnormality of the skeletal system (HP:0000924)),Set(),false), OntologicalTermWithParents_ES(Abnormality of limb bone morphology (HP:0002813),List(Abnormal appendicular skeleton morphology (HP:0011844), Abnormality of limb bone (HP:0040068)),Set(),false))), (participant_id_2,Phenotype_ES(None,None,Some(2),None,Some(Abnormality of the skeletal system (HP:0000924)),Some(Abnormality of the skeletal system (HP:0000924)),None,None,None,Some(source),Some(true)),List(OntologicalTermWithParents_ES(Abnormality of the skeletal system (HP:0000924),List(Phenotypic abnormality (HP:0000118)),Set(),false), OntologicalTermWithParents_ES(Phenotypic abnormality (HP:0000118),List(All (HP:0000001)),Set(),false), OntologicalTermWithParents_ES(All (HP:0000001),List(),Set(),false))), (participant_id_2,Phenotype_ES(None,None,Some(4),None,None,None,None,None,Some(SNOMED:1),Some(source),Some(true)),List()), (participant_id_5,Phenotype_ES(None,None,Some(7),None,None,None,None,None,None,None,Some(true)),List()), (participant_id_6,Phenotype_ES(None,None,Some(8),None,None,None,None,None,None,None,Some(false)),List()))
    //List(
    // (participant_id_1,Phenotype_ES(None,None,Some(1),Some(Arachnodactyly (HP:0001166)),None,None,None,None,None,None,Some(false)),List(OntologicalTermWithParents_ES(Arachnodactyly (HP:0001166),List(Slender finger (HP:0001238), Long fingers (HP:0100807)),Set(),true))), (participant_id_1,Phenotype_ES(None,None,Some(3),None,None,None,None,Some(SNOMED:1),None,None,Some(false)),List()), (participant_id_2,Phenotype_ES(None,None,Some(2),None,Some(Abnormality of the skeletal system (HP:0000924)),Some(Abnormality of the skeletal system (HP:0000924)),None,None,None,Some(source),Some(true)),List(OntologicalTermWithParents_ES(Abnormality of the skeletal system (HP:0000924),List(Phenotypic abnormality (HP:0000118)),Set(),false))), (participant_id_2,Phenotype_ES(None,None,Some(4),None,None,None,None,None,Some(SNOMED:1),Some(source),Some(true)),List()), (participant_id_5,Phenotype_ES(None,None,Some(7),None,None,None,None,None,None,None,Some(true)),List()), (participant_id_6,Phenotype_ES(None,None,Some(8),None,None,None,None,None,None,None,Some(false)),List()))
  }

  "process" should "merge phenotypes and their ancestors" in {
    val p1 = Participant_ES(kf_id = Some("participant_id_1"))
    val phenotype_11 = EPhenotype(kf_id = Some("phenotype_id_11"), age_at_event_days = Some(15), hpo_id_phenotype = Some("HP:0009654"), observed = Some("positive"), participant_id = Some("participant_id_1"), external_id = Some("phenotype 11"))
    val phenotype_16 = EPhenotype(kf_id = Some("phenotype_id_11"), age_at_event_days = None, hpo_id_phenotype = Some("HP:0000175"), observed = Some("positive"), participant_id = Some("participant_id_1"), external_id = Some("phenotype 11"))
    val phenotype_13 = EPhenotype(kf_id = Some("phenotype_id_13"), age_at_event_days = Some(18), hpo_id_phenotype = Some("HP:0045009"), observed = Some("positive"), participant_id = Some("participant_id_1"), external_id = Some("phenotype 13"))
    val phenotype_14 = EPhenotype(kf_id = Some("phenotype_id_14"), age_at_event_days = Some(9999), hpo_id_phenotype = Some("HP:0031816"), participant_id = Some("participant_id_1"), external_id = Some("phenotype 14")) // observed in None
    val phenotype_15 = EPhenotype(kf_id = Some("phenotype_id_15"), age_at_event_days = Some(22), hpo_id_phenotype = Some("HP:0011879"), observed = Some("negative"), participant_id = Some("participant_id_1"), external_id = Some("phenotype 15")) // Not observed
    val phenotype_12 = EPhenotype(kf_id = Some("phenotype_id_12"), participant_id = Some("participant_id_1"), external_id = Some("phenotype 12")) // observed in None

    val p2 = Participant_ES(kf_id = Some("participant_id_2"))
    val phenotype_2 = EPhenotype(kf_id = Some("phenotype_id_2"), participant_id = Some("participant_id_2"), external_id = Some("phenotype 2"))

    val phenotype_3 = EPhenotype(kf_id = Some("phenotype_id_3"), external_id = Some("phenotype 3"))

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
      OntologicalTermWithParents_ES(name = hpo_0000175.toString, parents = Seq(hpo_0000202.toString, hpo_0100737.toString), age_at_event_days = Set.empty[Int], is_tagged = true),
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
      OntologicalTermWithParents_ES(name = hpo_0009654.toString, parents = Seq(hpo_0009602.toString, hpo_0009771.toString), age_at_event_days = Set(15), is_tagged = true),
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
      OntologicalTermWithParents_ES(name = hpo_0045009.toString, parents = Seq(hpo_0002818.toString, hpo_0011314.toString, hpo_0040073.toString), age_at_event_days = Set(18), is_tagged = true),
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
      OntologicalTermWithParents_ES(name = hpo_0011879.toString, parents = Seq(hpo_0011878.toString), age_at_event_days = Set(22), is_leaf = true, is_tagged = true),
      OntologicalTermWithParents_ES(name = hpo_0011878.toString, parents = Seq(hpo_0011869.toString), age_at_event_days = Set(22)),
      OntologicalTermWithParents_ES(name = hpo_0011869.toString, parents = Seq(hpo_0001872.toString), age_at_event_days = Set(22)),
      OntologicalTermWithParents_ES(name = hpo_0001872.toString, parents = Seq(hpo_0001871.toString), age_at_event_days = Set(22)),
      OntologicalTermWithParents_ES(name = hpo_0001871.toString, parents = Seq(hpo_0000118.toString), age_at_event_days = Set(22)),
      OntologicalTermWithParents_ES(name = hpo_0000118.toString, parents = Seq(hpo_0000001.toString), age_at_event_days = Set(22)),
      OntologicalTermWithParents_ES(name = hpo_0000001.toString, parents = Seq.empty[String], age_at_event_days = Set(22))
    )
  }
}
