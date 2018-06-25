package io.kf.etl.context

import java.net.URL

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

  private def getIndexSuffix():Option[String] = {
    cli.hasOption("index_suffix") match {
      case true => {
        Some(cli.getOptionValue("index_suffix"))
      }
      case false => None
    }
  }


}

