package io.kf.etl.pipeline.internal.operators

import java.util.concurrent.{CountDownLatch, TimeUnit}

import io.kf.etl.pipeline.Pipeline
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}

class PipelineCombine[T, A1, A2](val source: Pipeline[T], p1: Function1[T, A1], p2: Function1[T, A2]) extends Pipeline[(A1, A2)]{
  override def run(): (A1, A2) = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val latch = new CountDownLatch(2)

    val input = source.run()

    def computeO1(): Future[A1] = {
      val promise_o1 = Promise[A1]

      Future{
        promise_o1.success(
          p1(input)
        )
        latch.countDown()
      }

      promise_o1.future
    }

    def computeO2(): Future[A2] = {
      val promise_o2 = Promise[A2]

      Future{
        promise_o2.success(
          p2(input)
        )
        latch.countDown()
      }
      promise_o2.future
    }

    val f1 = computeO1()
    val f2 = computeO2()
    latch.await()

    val future =
      for{
        a1 <- f1
        a2 <- f2
      } yield (a1, a2)

    Await.result(future, Duration(1, TimeUnit.HOURS))

  }

}
