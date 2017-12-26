package io.kf.etl.processor.api

//trait Processor[I, O] {
//  type ProcessInput
//  type ProcessOutput
//  type source <: Source[I, ProcessInput]
//  type sink <: Sink[ProcessOutput, O]
//
//  def id():String
//  def process(dataset: ProcessInput):ProcessOutput
//}

class Processor[T](val instance:T){
  def flatMap[B](func:T =>Processor[B]): Processor[B] = {
    func(instance)
  }
}

object Processor {
  def apply[T](t:T): Processor[T]= {
    new Processor(t)
  }
}
