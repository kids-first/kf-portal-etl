package io.kf.etl.models.internal

import io.kf.etl.models.es.{Biospecimen_ES, Participant_ES}

final case class BiospecimenES_ParticipantES(bio : Biospecimen_ES, participant : Participant_ES)
