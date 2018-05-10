package io.kf.etl.context

import org.apache.commons.cli.{BasicParser, CommandLine, OptionBuilder, Options}

class CLIParametersHolder(val args:Array[String]) {

  lazy val study_ids = getStudyIds()
  lazy val index_suffix = getIndexSuffix()

  private lazy val cli = parse()

  private def parse(): CommandLine = {

    val options = new Options

    OptionBuilder.hasArgs
    OptionBuilder.isRequired(false)
    OptionBuilder.withDescription("a list of study id")
    options.addOption(OptionBuilder.create("study_id"))

    OptionBuilder.hasArg
    OptionBuilder.isRequired(false)
    OptionBuilder.withDescription("suffix string for index name")
    options.addOption(OptionBuilder.create("index_suffix"))


    val parser = new BasicParser
    parser.parse(options, args)

  }

  private def getStudyIds(): Option[Array[String]] = {
    cli.hasOption("study_id") match {
      case true => {
        Some(cli.getOptionValues("study_id"))
      }
      case false => None
    }
  }

  private def getIndexSuffix():Option[String] = {
    cli.hasOption("index_suffix") match {
      case true => {
        Some(cli.getOptionValue("study_id"))
      }
      case false => None
    }
  }


}

