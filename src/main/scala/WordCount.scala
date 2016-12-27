package WordCount

import scala.collection.mutable
import scala.io.Source

import Main.MapReduce

object WordCount {
  def mapper(f: String): Map[String, Int] = {
    val r = new mutable.HashMap[String, Int]()
    for (line: String <- Source.fromFile(f).getLines()) {
      val words = line.split(" ").filter(_.length > 3).map(_.toUpperCase)
      words.foreach { w =>
        r.put(w, r.getOrElse(w, 0) + 1)
      }
    }
    r.toMap
  }
  def shuffler(x: String, y: Any) = x
  def reducer(k: Any, v: List[Int]) = v.foldLeft(0)(_ + _)
  def doIt(files: List[String]): Map[String, Int] =
    MapReduce.mapReduce[String, String, Int, String, Int](2, mapper, 4, shuffler, 3, reducer)(files)
}
