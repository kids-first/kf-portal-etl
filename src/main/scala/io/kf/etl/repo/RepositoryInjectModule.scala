package io.kf.etl.repo

import com.google.inject.{AbstractModule, Scope, Scopes}
import io.kf.etl.inject.GuiceModule
import io.kf.etl.repo.impl.LocalHttpServerRepository

@GuiceModule
class RepositoryInjectModule extends AbstractModule{
  override def configure(): Unit = {
    bind(classOf[Repository]).to(classOf[LocalHttpServerRepository]).in(Scopes.SINGLETON)
  }

}
