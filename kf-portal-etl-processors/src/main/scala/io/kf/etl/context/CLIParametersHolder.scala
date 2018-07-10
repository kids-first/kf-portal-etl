package io.kf.etl.context

import java.net.URL

import org.apache.commons.cli.{BasicParser, CommandLine, OptionBuilder, Options}

class CLIParametersHolder(val args:Array[String]) {

  lazy val study_ids = getStudyIds()
  lazy val release_id = getReleaseId()

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

  private def getStudyIds(): Option[Array[String]] = {
    val arr =
      (cli.hasOption("study_id") match {
        case true => {
          cli.getOptionValues("study_id")
        }
        case false => Array.empty[String]
      }) ++
      (cli.hasOption("study_id_file") match {
        case true => {
          StudyIdFileParser.getStudyIDs(new URL(cli.getOptionValue("study_id_file") ))
        }
        case false => Array.empty[String]
      })
    arr.isEmpty match {
      case true => None
      case false => Some(arr)
    }
  }

  private def getReleaseId():Option[String] = {
    cli.hasOption("release_id") match {
      case true => {
        Some(cli.getOptionValue("release_id"))
      }
      case false => None
    }
  }


}

