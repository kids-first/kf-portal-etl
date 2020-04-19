package io.kf.etl.models.internal

import io.kf.etl.models.es.SequencingExperiment_ES

case class SequencingExperimentsES_GenomicFileId (
                                                   sequencing_experiments : Seq[SequencingExperiment_ES],
                                                   genomic_file_id : String
                                                 )
