package io.kf.etl.models.internal

import io.kf.etl.models.es.{Biospecimen_ES, GenomicFile_ES}

case class BiospecimenES_GenomicFileES(bio : Biospecimen_ES, genomicFile : GenomicFile_ES)
