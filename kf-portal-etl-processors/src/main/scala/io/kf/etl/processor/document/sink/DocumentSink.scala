package io.kf.etl.processor.document.sink

import io.kf.etl.processor.document.context.DocumentContext
import io.kf.etl.model.FileCentric
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Dataset

class DocumentSink(val context: DocumentContext) {
  def sink(data:Dataset[FileCentric]):Unit = {
    context.hdfs.delete(new Path(context.getJobDataPath()), true)
    data.write.parquet(context.getJobDataPath())
  }
}
