package io.kf.etl.processor.document

import io.kf.model.Doc
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Dataset

class DocumentJobSink {
  def sink(data:Dataset[Doc]):Unit = {
    val context: DocumentJobContext = ???

    context.hdfs.delete(new Path(context.getJobDataPath()), true)
    data.write.parquet(context.getJobDataPath())
  }
}