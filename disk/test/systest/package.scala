import java.util.concurrent.ConcurrentSkipListSet
import com.treode.async.AsyncIterator

package systest {

  case class TestConfig (targetPageBytes: Int)
}

package object systest {

  type MemTier = ConcurrentSkipListSet [Cell]
  type CellIterator = AsyncIterator [Cell]

  val emptyMemTier = new ConcurrentSkipListSet [Cell] (Cell)
  def newMemTier = new ConcurrentSkipListSet [Cell] (Cell)
}
