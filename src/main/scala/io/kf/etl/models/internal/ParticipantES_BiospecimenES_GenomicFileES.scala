package io.kf.etl.models.internal

import io.kf.etl.models.es.{Biospecimen_ES, GenomicFile_ES, Participant_ES}

case class ParticipantES_BiospecimenES_GenomicFileES(bio : Biospecimen_ES, genomicFile : GenomicFile_ES, participant : Participant_ES)
