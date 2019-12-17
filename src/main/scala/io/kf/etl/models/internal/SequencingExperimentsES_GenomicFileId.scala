package io.kf.etl.models.internal

import io.kf.etl.models.es.SequencingExperiment_ES

case class SequencingExperimentsES_GenomicFileId (
                                                   sequencingExperiments : Seq[SequencingExperiment_ES],
                                                   genomicFileId : String
                                                 )
