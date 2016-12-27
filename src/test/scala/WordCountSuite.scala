package WordCount

import scala.collection.immutable

import org.scalatest.FunSuite

class WordCountTest extends FunSuite {
  test("WordCount counts properly") {
    assert(immutable.HashMap[String, Int](
      "CRUEL" -> 2, "WERELD" -> 1, "WORLD" -> 2,
      "ADIEU" -> 1, "MONDE" -> 2, "BONJOUR" -> 1, "HELLO" -> 1)
      == WordCount.doIt(
        List("src/test/data/f1.txt", "src/test/data/f2.txt")))
    0
  }
}
