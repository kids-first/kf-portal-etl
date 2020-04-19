package io.kf.etl.models.internal

import io.kf.etl.models.es.SequencingExperiment_ES

case class SequencingExperimentES_GenomicFileId (
                                                  sequencing_experiment : SequencingExperiment_ES,
                                                  genomic_file_id : Option[String]
                                                )
