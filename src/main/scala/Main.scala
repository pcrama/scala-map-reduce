package Main

import scala.collection.{mutable, immutable}
import scala.io.Source

object MapReduceTypes {
  type MapFunction[Inp, MapKey, Val] = (Inp) => Map[MapKey, Val]
  type ShuffleFunction[MapKey, ShuffleKey, Val] = (MapKey, Val) => ShuffleKey
  type ReduceFunction[ShuffleKey, Val, Result] = (ShuffleKey, List[Val]) => Result
}

class Mapper[Inp, MapKey, Val](mapF: MapReduceTypes.MapFunction[Inp, MapKey, Val]) {
  def run(input: Inp): Map[MapKey, Val] = mapF(input)
}

class Shuffler[ShuffleKey, Val] {
  private val mutMap = mutable.HashMap() : mutable.HashMap[ShuffleKey, List[Val]]
  def accumulate(k: ShuffleKey, v: Val): Unit = {
    mutMap.put(k, v :: mutMap.getOrElse(k, Nil))
  }
  def freeze: Map[ShuffleKey, List[Val]] = mutMap.toMap
}

class Reducer[ShuffleKey, Val, Result](reduceF: MapReduceTypes.ReduceFunction[ShuffleKey, Val, Result]) {
  def run(data: Map[ShuffleKey, List[Val]]): Map[ShuffleKey, Result] = {
    new immutable.HashMap() ++ { for ((k, v) <- data) yield { (k, reduceF(k, v)) } }
  }
}

object MapReduce {
  def mapReduce[Inp, MapKey, Val, ShuffleKey, Result](
    mapperCount: Int, mapF: MapReduceTypes.MapFunction[Inp, MapKey, Val],
    shufflerCount: Int, shuffleF: MapReduceTypes.ShuffleFunction[MapKey, ShuffleKey, Val],
    reducerCount: Int, reduceF: MapReduceTypes.ReduceFunction[ShuffleKey, Val, Result]
  )(input: List[Inp]): Map[ShuffleKey, Result] = {
    require((mapperCount > 0) && (shufflerCount > 0) && (reducerCount > 0))
    val mappers = makeN(mapperCount, _ => new Mapper(mapF))
    // Distribute workload, input is expected to be large -> should enumerate
    // as lazy list (.view), but got type errors...
    val mapResults = input.zipWithIndex.map {
      case (inp, idx) => mappers(idx % mapperCount).run(inp) }
    // Rearrange
    val shufflerResults: List[Map[ShuffleKey, List[Val]]] = shuffle(shufflerCount, shuffleF, mapResults)
    // Reduce
    val reducers = makeN(reducerCount, _ => new Reducer(reduceF))
    val reduceResults = shufflerResults.zipWithIndex.map {
      case (inp, idx) => reducers(idx % reducerCount).run(inp)
    }
    // Merge all reduce's results
    reduceResults.foldLeft(new immutable.HashMap(): Map[ShuffleKey, Result])(
      _ ++ _)
  }
  private def makeN[A](n: Int, f: Unit => A): List[A] = List.tabulate(n)(_ => f(()))
  private def shuffle[MapKey, ShuffleKey, Val](
    shufflerCount: Int,
    shuffleF: MapReduceTypes.ShuffleFunction[MapKey, ShuffleKey, Val],
    mapResults: List[Map[MapKey, Val]]
  ): List[Map[ShuffleKey, List[Val]]] = {
    val shufflers = makeN(shufflerCount, _ => new Shuffler(): Shuffler[ShuffleKey, Val])
    for (subMap <- mapResults) {
      for ((k, v) <- subMap) {
        val newKey = shuffleF(k, v)
        shufflers(newKey.hashCode().abs % shufflerCount).accumulate(newKey, v)
      }
    }
    shufflers.map(_.freeze)
  }
}

object Main extends App {
  println("Hello World")
}
