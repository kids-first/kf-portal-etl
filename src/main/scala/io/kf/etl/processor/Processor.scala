package io.kf.etl.processor

class Processor[INPUT](val instance:INPUT){
  def flatMap[ENTITY1, ENTITY2, OUTPUT](source: INPUT => ENTITY1)(transform:ENTITY1 => ENTITY2)(sink: ENTITY2 => OUTPUT): Processor[OUTPUT] = {
    Processor( SubProcessor(instance).flatMap(a => SubProcessor(source(a))).flatMap(a => SubProcessor(transform(a))).flatMap(a => SubProcessor(sink(a))).input )
  }
}

object Processor {
  def apply[INPUT](t:INPUT): Processor[INPUT]= {
    new Processor(t)
  }
}


class SubProcessor[INPUT](val input: INPUT) {
  def flatMap[OUTPUT](func: INPUT => SubProcessor[OUTPUT]): SubProcessor[OUTPUT] = {
    func(input)
  }
}

object SubProcessor {
  def apply[INPUT](t:INPUT): SubProcessor[INPUT]= {
    new SubProcessor(t)
  }
}