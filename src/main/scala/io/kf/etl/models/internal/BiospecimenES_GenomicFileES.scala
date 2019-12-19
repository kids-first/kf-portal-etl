package io.kf.etl.models.internal

import io.kf.etl.models.es.{BiospecimenCombined_ES, GenomicFile_ES}

case class BiospecimenES_GenomicFileES(bio : BiospecimenCombined_ES, genomicFile : GenomicFile_ES)
