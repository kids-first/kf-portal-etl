package io.kf.etl.models.internal

import io.kf.etl.models.es.{BiospecimenCombined_ES, Participant_ES}

final case class BiospecimenES_ParticipantES(bio : BiospecimenCombined_ES, participant : Participant_ES)
