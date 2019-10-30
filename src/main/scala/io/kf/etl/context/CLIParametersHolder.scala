package io.kf.etl.context

import org.apache.commons.cli.{BasicParser, CommandLine, OptionBuilder, Options}

class CLIParametersHolder(val args: Array[String]) {

  lazy val study_ids: Option[Array[String]] = getStudyIds
  lazy val release_id: Option[String] = getReleaseId

  private lazy val cli = parse()

  private def parse(): CommandLine = {

    val options = new Options

    OptionBuilder.hasArgs
    OptionBuilder.isRequired(false)
    OptionBuilder.withDescription("a list of study id")
    options.addOption(OptionBuilder.create("study_id"))

    OptionBuilder.hasArg
    OptionBuilder.isRequired(false)
    OptionBuilder.withDescription("release id appended to index name")
    options.addOption(OptionBuilder.create("release_id"))

    OptionBuilder.hasArg
    OptionBuilder.isRequired(false)
    OptionBuilder.withDescription("a file which contains the study ids and is represented by a URL")
    options.addOption(OptionBuilder.create("study_id_file"))

    val parser = new BasicParser
    parser.parse(options, args)

  }

  private def getStudyIds: Option[Array[String]] = {
    if (cli.hasOption("study_id")) {
      Some(cli.getOptionValues("study_id"))
    } else {
      None
    }
  }

  private def getReleaseId: Option[String] = {
    if (cli.hasOption("release_id")) {
      Some(cli.getOptionValue("release_id"))
    } else {
      None
    }
  }


}

