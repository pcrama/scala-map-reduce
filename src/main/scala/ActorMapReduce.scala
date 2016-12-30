package ActorMapReduce.scala
// http://alvinalexander.com/scala/scala-akka-actors-ping-pong-simple-example
import akka.actor.Actor
import akka.actor._

class MapReduceTypes[Inp, MapKey, ShuffleKey, Val, TempResult, Result] {
  type MapFunction = (Inp) => Iterator[(MapKey, Val)]
  type ShuffleFunction = (MapKey, Val) => ShuffleKey
  type ReduceFunction = (ShuffleKey, List[Val]) => TempResult
  type PostprocessFunction = (ShuffleKey, TempResult) => Result

  abstract class MapReduceMessage
  case class MapperInput(inp: Inp) extends MapReduceMessage
  case class MapperOutput(key: MapKey, value: Val) extends MapReduceMessage
  case class ShufflerOutput(key: ShuffleKey, value: Val) extends MapReduceMessage
  case class ReducerOutput(key: ShuffleKey, value: TempResult) extends MapReduceMessage
  case class PostprocessorOutput(key: ShuffleKey, value: Result) extends MapReduceMessage

  class Mapper(mapF: MapFunction, shuffler: Actor) extends Actor {
    def receive(): Unit = {
      loop {
        react {
          case MapperInput(x) => for((k, v) <- mapF(x)) { shuffler ! (k, v) }
        }
      }
    }
  }

  class Shuffler(shuffleF: ShuffleFunction, reducers: IndexedSeq[Actor]) {
    val reducerCount = reducers.length
    def act(): Unit = {
      loop {
        react {
          case MapperOutput(k, v) => {
            val newKey = shuffleF(k, v)
            val c = newKey.hashCode() % reducerCount
            reducers(if (c >= 0) c else reducerCount + c) ! (newKey, v)
          }
        }
      }
    }
  }
}


