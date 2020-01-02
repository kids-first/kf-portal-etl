package io.kf.etl.models.internal

import io.kf.etl.models.es.Biospecimen_ES

final case class BiospecimenCombinedES_GenomicFileId(bio: Biospecimen_ES, gfId: String)
