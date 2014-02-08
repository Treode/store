import java.util.concurrent.ConcurrentSkipListMap
import scala.collection.JavaConversions._
import com.treode.async.AsyncIterator

package systest {

  case class TestConfig (targetPageBytes: Int)
}

package object systest {

  type MemTier = ConcurrentSkipListMap [Int, Option [Int]]
  type CellIterator = AsyncIterator [Cell]

  val emptyMemTier = new ConcurrentSkipListMap [Int, Option [Int]]
  def newMemTier = new ConcurrentSkipListMap [Int, Option [Int]]
}
