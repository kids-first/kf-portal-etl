package io.kf.etl.processor.index

import io.kf.etl.processor.common.Processor
import io.kf.etl.processor.index.context.IndexContext
import io.kf.etl.processor.repo.Repository
import org.apache.spark.sql.Dataset

class IndexProcessor(context: IndexContext,
                     source: ((String, Repository)) => (String, Dataset[String]),
                     transform: ((String, Dataset[String])) => (String, Dataset[String]),
                     sink: ((String, Dataset[String])) => Unit) extends Processor[(String, Repository), Unit]{

  def process(input: (String, Repository)):Unit = {
    source.andThen(transform).andThen(sink)(input)

  }

}
