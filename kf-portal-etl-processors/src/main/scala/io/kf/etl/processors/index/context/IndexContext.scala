package io.kf.etl.processors.index.context

import io.kf.etl.common.conf.ESConfig
import io.kf.etl.context.Context
import io.kf.etl.processors.common.processor.{ProcessorConfig, ProcessorContext}
import io.kf.etl.processors.index.transform.releasetag.ReleaseTag

class IndexContext(override val appContext: Context,
                   override val config: IndexConfig) extends ProcessorContext {
}


case class IndexConfig(override val name:String, esConfig: ESConfig, override val dataPath:Option[String], aliasActionEnabled: Boolean, aliasHandlerClass:String, releaseTag: ReleaseTag) extends ProcessorConfig