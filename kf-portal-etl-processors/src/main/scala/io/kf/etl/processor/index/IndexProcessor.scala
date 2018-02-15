package io.kf.etl.processor.index

import io.kf.etl.processor.common.Processor
import io.kf.etl.processor.index.context.IndexContext
import io.kf.etl.processor.repo.Repository
import io.kf.etl.model.DocType
import org.apache.spark.sql.Dataset

class IndexProcessor(context: IndexContext,
                     source: Repository => Dataset[DocType],
                     transform: Dataset[DocType] => Dataset[DocType],
                     sink: Dataset[DocType] => Unit) extends Processor[Repository, Unit]{

  def process(input: Repository):Unit = {
    source.andThen(transform).andThen(sink)(input)

  }

}
