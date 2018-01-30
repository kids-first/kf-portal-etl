package io.kf.etl.processor.download.sink

import io.kf.etl.processor.download.context.DownloadContext
import io.kf.model.Doc
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Dataset

class DownloadSink(val context: DownloadContext) {
  def sink(data:Dataset[Doc]):Unit = {

    context.hdfs.delete(new Path(context.getJobDataPath()), true)

    data.write.parquet(context.getJobDataPath())
  }
}
