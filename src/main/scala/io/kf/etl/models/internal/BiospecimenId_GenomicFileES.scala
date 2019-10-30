package io.kf.etl.models.internal

import io.kf.etl.models.es.GenomicFile_ES

final case class BiospecimenId_GenomicFileES(bioId: String, file: GenomicFile_ES)
