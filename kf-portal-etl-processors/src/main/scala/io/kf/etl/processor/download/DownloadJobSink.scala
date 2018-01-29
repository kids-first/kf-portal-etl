package io.kf.etl.processor.download

import io.kf.model.Doc
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Dataset

class DownloadJobSink(val context: DownloadJobContext) {
  def sink(data:Dataset[Doc]):Unit = {

    context.hdfs.delete(new Path(context.getJobDataPath()), true)

    data.write.parquet(context.getJobDataPath())
  }
}
