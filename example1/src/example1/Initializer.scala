package example1

import java.nio.file.Paths
import com.treode.cluster.{CellId, HostId}
import com.treode.store.StandAlone
import com.twitter.app.App
import com.twitter.conversions.storage._
import com.twitter.util.StorageUnit

class Initializer extends App {

  val cell = flag [CellId] ("cell", "Cell ID")

  val host = flag [HostId] ("host", "Host ID")

  val superBlockBits =
      flag [Int] ("superBlockBits",  14, "Size of the super block (log base 2)")

  val segmentBits =
      flag [Int] ("segmentBits", 30, "Size of a disk segment (log base 2)")

  val blockBits =
      flag [Int] ("blockBits", 13, "Size of a disk block (log base 2)")

  val diskBytes =
      flag [StorageUnit] ("diskBytes", 1.terabyte, "Maximum size of disk (bytes)")

  def main() {

    if (!cell.isDefined || !host.isDefined) {
      println ("-cell and -host are required.")
      return
    }

    if (args.length == 0) {
      println ("At least one path is required.")
      return
    }

    val paths = args map (Paths.get (_))

    StandAlone.init (
        host(),
        cell(),
        superBlockBits(),
        segmentBits(),
        blockBits(),
        diskBytes().inBytes,
        paths: _*)
  }}
