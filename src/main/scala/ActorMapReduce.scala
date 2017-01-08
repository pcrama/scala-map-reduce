package ActorMapReduce.scala
// Sorry, this isn't complete, but time's up...

import scala.collection.mutable
// http://alvinalexander.com/scala/scala-akka-actors-ping-pong-simple-example
import akka.actor._

class MapReduceTypes[Inp, MapKey, ShuffleKey, Val, TempResult, Result] {
  type MapFunction = (Inp) => Iterator[(MapKey, Val)]
  type ShuffleFunction = (MapKey, Val) => ShuffleKey
  type ReduceFunction = (ShuffleKey, TempResult, Val) => TempResult
  type PostprocessFunction = (ShuffleKey, TempResult) => Result

  abstract class MapReduceMessage
  case class MapperInput(inp: Inp) extends MapReduceMessage
  case class MapperOutput(key: MapKey, value: Val) extends MapReduceMessage
  case class ShufflerOutput(key: ShuffleKey, value: Val) extends MapReduceMessage
  case class ReducerOutput(key: ShuffleKey, value: TempResult) extends MapReduceMessage
  case class PostprocessorOutput(result: Iterable[(ShuffleKey, Result)]) extends MapReduceMessage
  case class Done() extends MapReduceMessage
  case class AllInputsSent() extends MapReduceMessage
  case class OneInputCompleted() extends MapReduceMessage
  case class CloseShufflers() extends MapReduceMessage
  case class ShufflerClosed() extends MapReduceMessage
  case class CloseReducers() extends MapReduceMessage
  case class ReducerClosed() extends MapReduceMessage

  def roundRobinSend(receivers: IndexedSeq[ActorRef], k: Any, x: MapReduceMessage): Unit = {
    val code = k.hashCode() % receivers.length
    receivers(if (code >= 0) code else receivers.length + code) ! x
  }

  class Mapper(mapF: MapFunction, shufflers: IndexedSeq[ActorRef]) extends Actor {
    def receive: MapReduceMessage => Unit = {
      case MapperInput(x) =>
        for((k, v) <- mapF(x)) {
          roundRobinSend(shufflers, k, MapperOutput(k, v))
        }
        sender ! OneInputCompleted
    }
  }

  class Shuffler(shuffleF: ShuffleFunction, reducers: IndexedSeq[ActorRef]) extends Actor {
    def receive: MapReduceMessage => Unit = {
      case MapperOutput(k, v) =>
        val newKey = shuffleF(k, v)
        roundRobinSend(reducers, newKey, ShufflerOutput(newKey, v))
      case CloseShufflers() =>
        sender ! ShufflerClosed
    }
  }

  class Reducer(reduceF: ReduceFunction, initTempResult: TempResult, postProcessF: PostprocessFunction, finalReceiver: ActorRef) extends Actor {
    val tempResults = mutable.Map[ShuffleKey, TempResult]()
    def receive: MapReduceMessage => Unit = {
      case ShufflerOutput(k, v) =>
        tempResults(k) = reduceF(k, tempResults.getOrElse(k, initTempResult), v)
      case CloseShufflers() =>
        finalReceiver ! PostprocessorOutput(
          for ((k, v) <- tempResults) yield (k, postProcessF(k, v))
        )
        sender ! ShufflerClosed
    }
  }

  class FinalReceiver() extends Actor {
    val finalResults = mutable.Map[ShuffleKey, Result]()
    def receive: MapReduceMessage => Unit = {
      case PostprocessorOutput(iterable) =>
        for ((k, v) <- iterable) { finalResults(k) = v }
      case Done() =>
        sender ! finalResults
    }
  }

  class Driver(mappers: Array[ActorRef], shufflers: Array[ActorRef], reducers: Array[ActorRef], finalReceiver: ActorRef, result: mutable.Map[ShuffleKey, Result]) extends Actor {
    var inputsSent = 0
    var inputsCompleted = 0
    val mapperCount = mappers.length
    var waitForEnd = false
    var closedShufflers = 0
    var closedReducers = 0
    def receive: MapReduceMessage => Unit = {
      case MapperInput(inp) =>
        mappers(inputsSent % mapperCount) ! MapperInput(inp)
        inputsSent += 1
      case AllInputsSent() =>
        waitForEnd = true
      case OneInputCompleted() =>
        inputsCompleted += 1
        if (waitForEnd && inputsCompleted >= inputsSent) {
          for (shuf <- shufflers) { shuf ! CloseShufflers }
        }
      case ShufflerClosed() =>
        closedShufflers += 1
        if (closedShufflers >= shufflers.length) {
          for (red <- reducers) {
            red ! CloseReducers
          }
        }
      case ReducerClosed() =>
        closedReducers += 1
        if (closedReducers >= reducers.length) {
          finalReceiver ! Done
        }
    }
  }

  def runMapReduce(input: Iterable[Inp], mapF: MapFunction, mapperCount: Int, shuffleF: ShuffleFunction, shufflerCount: Int, reduceF: ReduceFunction, initTempResult: TempResult, postprocessF: PostprocessFunction, reducerCount: Int): mutable.Map[ShuffleKey, Result] = {
    require(mapperCount > 0 && shufflerCount > 0 && reducerCount > 0)
    var c = 0 // counter to generate unique actor names
    val system = ActorSystem("MapReduceSystem")
    val finalReceiver = system.actorOf(Props(new FinalReceiver), name = "final")
    val reducers = Array.fill(reducerCount)(
      system.actorOf(Props(new Reducer(reduceF, initTempResult, postprocessF, finalReceiver)), name = { c += 1; "R" + c.toString() } ))
    val shufflers = Array.fill(shufflerCount)(
      system.actorOf(Props(new Shuffler(shuffleF, reducers)), name = { c += 1; "S" + c.toString() }))
    val mappers = Array.fill(mapperCount)(system.actorOf(Props(new Mapper(mapF, shufflers)), name = { c += 1; "M" + c.toString() }))
    val driver = system.actorOf(Props(new Driver(mappers, shufflers, reducers, finalReceiver, mutable.Map[ShuffleKey, Result]())), name = "Driver")
    for (inp <- input) { driver ! MapperInput(inp) }
    driver.result
  }
}
