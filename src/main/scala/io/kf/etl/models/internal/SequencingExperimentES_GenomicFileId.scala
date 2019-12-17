package io.kf.etl.models.internal

import io.kf.etl.models.es.SequencingExperiment_ES

case class SequencingExperimentES_GenomicFileId (
                                                  sequencingExperiment : SequencingExperiment_ES,
                                                  genomicFileId : Option[String]
                                                )
