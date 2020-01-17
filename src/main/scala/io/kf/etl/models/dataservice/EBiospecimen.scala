package io.kf.etl.models.dataservice

case class EBiospecimen(
                         ageAtEventDays: Option[Int] = None,
                         analyteType: Option[String] = None,
                         composition: Option[String] = None,
                         concentrationMgPerMl: Option[Double] = None,
                         consentType: Option[String] = None,
                         createdAt: Option[String] = None,
                         duoIds: Seq[String] = Nil,
                         dbgapConsentCode: Option[String] = None,
                         externalAliquotId: Option[String] = None,
                         externalSampleId: Option[String] = None,
                         kfId: Option[String] = None,
                         methodOfSampleProcurement: Option[String] = None,
                         modifiedAt: Option[String] = None,
                         ncitIdAnatomicalSite: Option[String] = None,
                         ncitIdTissueType: Option[String] = None,
                         shipmentDate: Option[String] = None,
                         shipmentOrigin: Option[String] = None,
                         genomicFiles: Seq[String] = Nil,
                         participantId: Option[String] = None,
                         sourceTextTumorDescriptor: Option[String] = None,
                         sourceTextTissueType: Option[String] = None,
                         sourceTextAnatomicalSite: Option[String] = None,
                         spatialDescriptor: Option[String] = None,
                         uberonIdAnatomicalSite: Option[String] = None,
                         volumeMl: Option[Double] = None,
                         sequencingCenterId: Option[String] = None,
                         diagnoses: Seq[EDiagnosis] = Nil,
                         visible: Option[Boolean] = None
    )
