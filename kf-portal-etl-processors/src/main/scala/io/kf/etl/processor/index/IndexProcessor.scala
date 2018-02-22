package io.kf.etl.processor.index

import io.kf.etl.processor.common.Processor
import io.kf.etl.processor.index.context.IndexContext
import io.kf.etl.processor.repo.Repository
import io.kf.etl.model.FileCentric
import org.apache.spark.sql.Dataset

class IndexProcessor(context: IndexContext,
                     source: Repository => Dataset[FileCentric],
                     transform: Dataset[FileCentric] => Dataset[FileCentric],
                     sink: Dataset[FileCentric] => Unit) extends Processor[Repository, Unit]{

  def process(input: Repository):Unit = {
    source.andThen(transform).andThen(sink)(input)

  }

}
