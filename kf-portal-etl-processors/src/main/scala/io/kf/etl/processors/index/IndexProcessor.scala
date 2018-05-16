package io.kf.etl.processors.index

import io.kf.etl.processors.common.processor.Processor
import io.kf.etl.processors.index.context.IndexContext
import io.kf.etl.processors.index.es.impl.DefaultIndexProcessorAdvice
import io.kf.etl.processors.repo.Repository
import org.apache.spark.sql.Dataset

class IndexProcessor(context: IndexContext,
                     source: ((String, Repository)) => (String, Dataset[String]),
                     transform: ((String, Dataset[String])) => (String, Dataset[String]),
                     sink: ((String, Dataset[String])) => Unit) extends Processor[(String, Repository), Unit]{

  def process(input: (String, Repository)):Unit = {

    context.config.adviceEnabled match {
      case true => {
        val advice = new DefaultIndexProcessorAdvice
        advice.preProcess(context, input._1)
        source.andThen(transform).andThen(sink)(input)
        advice.postProcess(context, input._1)
      }
      case false => {
        source.andThen(transform).andThen(sink)(input)
      }
    }


  }

}
