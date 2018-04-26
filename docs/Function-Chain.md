![](../images/watch_gears.jpg)
## Forword

In my previous post [Introducing the Kids-First ETL](http://softeng.oicr.on.ca/grant_guo/2018/03/19/kf-etl/), I mentioned:

> A nice future enhacement would be to **make the Pipeline programmable by defining operators** within Pipeline that accept Processor(s) as input and determine how to execute the Processor(s)

In this blog, Let's explore how to do it.

## The flaws of the current implementation

Let's take a look at how the **Pipeline** is implemented:

```
object Pipeline {
    private lazy val injector = createInjector()
    private def createInjector(): Injector = { ... }
    def run():Unit = {
        val download = injector.getInstance(classOf[DownloadProcessor])
        val filecentric = injector.getInstance(classOf[FileCentricProcessor])
        val participantcentric = injector.getInstance(classOf[ParticipantCentricProcessor])
        val index = injector.getInstance(classOf[IndexProcessor])
    
        val dump_location = download.process()
        
        val fp = filecentric.process _
        fp.andThen(index.process)(dump_location)
    
        val pp = participantcentric.process _
        pp.andThen(index.process)(dump_location)
    
    }
}

```

* Initialize all of the ```Processor```s
* Run ```Download``` processor to dump the data sources
* Run the subroutine: ```FileCentricProcessor``` -> ```IndexProcessor```
* Run the subroutine: ```ParticipantCentricProcessor``` -> ```IndexProcessor```

The main issue of the above implementation is **lack of flexibility**. 

Consider the following facts:

* Either ```andThen(...)``` or ```Function.chain(...)``` only handles the linear function calls, so every index has its own subroutine. Although they could perform the similar logic and share the same set of Processors, these subroutines still have to be defined independently，and the shared Processors have to be called explicitly in each subroutine.
* The semantics of ```andThen(...)``` or ```Function.chain(...)``` is not rich. For example, the ```FileCentric``` and ```ParticipantCentric``` subroutes run in sequence, however logically the two indices are independent of each other, so at least the two Processors ```FileCentricProcessor``` and ```ParticipantProcessor``` could be executed in parallel. In this case, Java/Scala's concurrent primitives or APIs should be introduced to program the concurrency. As more and more these kinds of control logic are added, the whole application will become the monolith and much more difficult to extend
* These control logic is reusable, but doesn't generate any new data or make the ```Pipeline``` transition to a new state, instead they organize the ```Processor```s to achieve the goal

## Refactor the Pipeline

The following code snippet is the new ```Pipeline``` implementation:

```
trait Pipeline[T] {

  def map[A](p: Function1[T, A]): Pipeline[A] = {
    new PipelineMapForFunction1[T, A](this, p)
  }
  def map[A](p: Processor[T, A]): Pipeline[A] = {
    new PipelineMapForProcessor[T, A](this, p)
  }
  def combine[A1, A2](p1: Processor[T, A1], p2: Processor[T, A2]): Pipeline[(A1, A2)] = {
    new PipelineCombine[T, A1, A2](this, p1, p2)
  }
  def merge[A1, A2, A3](p1: Processor[T, A1], p2: Processor[T, A2], merge_func: (A1, A2)=>A3): Pipeline[A3] = {
    new PipelineMerge[T, A1, A2, A3](this, p1, p2, merge_func)
  }
  def run(): T
}
```

The ```Pipeline```  is a parameterized Scala trait. The type parameter defines what data type the ```Pipeline``` holds. Transformation functions(```map```, ```combine```, ```merge``` and more in the future, I call them ```operator```) take ```Processor```(s) as input and coordinate the execution of them. By design, the ```Pipeline``` is late evaluated, which means the execution of operators doesn't compute any values, instead abstract function ```run()``` does. Each operator returns another ```Pipeline``` instance, which makes function call chain possible. The operators make the ```Pipleline``` look like a finite state machine(FSM), and the whole function call chain forms a directed acyclic graph(DAG). 

The ```Pipeline``` has the companion object, defined as the following:

```
object Pipeline {
  def from[O](p: Function1[Unit, O]): Pipeline[O] =  {
    new PipelineFromProcessor[O](p)
  }
  def from[T](s:T): Pipeline[T] = {
    new PipelineFrom[T](s)
  }
  def from[T1, T2](s1:T1, s2:T2): Pipeline[(T1, T2)] = {
    new PipelineFromTuple[T1, T2](s1, s2)
  }
  def from[T1, T2, T3](s1:T1, s2:T2, merge_func: (T1, T2) => T3): Pipeline[T3] = {
    new PipelineFromTupleWithMergeFunc[T1, T2, T3](s1, s2, merge_func)
  }
}
```

The functions defined in the companion object are also operators, serve like the static functions in the Java world, and they could be used as the starting point of a pipeline.

Now let's investigate the operators a little bit deeper

Each operator has its own semantics of how the ```Processor```(s) are executed, for exmaple:
* ```map``` takes one ```Processor``` to convert the instance of ```T``` into ```A```
* ```combine``` applies the two ```Processor```(s) to the instance of ```T``` respectively in parallel, and return a tuple of ```A1``` and ```A2```
* ```merge``` applies the two ```Processor```(s) to the instance of ```T``` respectively in parallel. Instead of return a tuple, it applies another merge function to the tuple

The implementation of the operator just returns an instance of the derived class from the ```Pipeline```, for example:

```
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
```

Here ```PipelineCombine``` uses Scala [Future](https://www.scala-lang.org/api/2.11.12/index.html#scala.concurrent.Future) and Java [CountDownLatch](https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/CountDownLatch.html). The developer could choose any mechanism only if adhering to the semantic definition of the operator.

Finally, ```ETLMain``` becomes 

```
object ETLMain extends App{

  private lazy val injector = createInjector()

  private def createInjector(): Injector = {

    ... ...
  }

  val download = injector.getInstance(classOf[DownloadProcessor])
  val filecentric = injector.getInstance(classOf[FileCentricProcessor])
  val participantcentric = injector.getInstance(classOf[ParticipantCentricProcessor])
  val index = injector.getInstance(classOf[IndexProcessor])

  Pipeline.from(download).combine(filecentric, participantcentric).map(tuples => {
    Seq(tuples._1, tuples._2).map(tuple => {
      index.process(tuple)
    })
  }).run()

}
``` 

## Summary

Through refactoring, the ```Pipeline``` is converted into a container type with transformation and action functions. We can see the similar ideas in [RxJava](https://github.com/ReactiveX/RxJava) and [Apache Spark](https://github.com/apache/spark). We could program the ```Pipeline``` by chaining the operators. Further, we even could have the different pipeline chaining logic for the different indices if needed. 

The refactoring leaves the hierarchy of ```Processor``` untouched, also reflects the idea of separation of concerns. The programming model is more in a functional way.

## Next step

So far, the hierarchy of the ```Pipeline``` is quite simple, as more and complex logic is added, more operators could be defined to make the ```Pipeline``` more programmable. 

The two facts, 1) the hierarchy of the ```Pipeline``` looks like a DAG; 2) the separation of the hierarchies of ```Pipeline``` and ```Processor```, make it possible for ETL to have the global scheduler and/or optimizer. 
 
