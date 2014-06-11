package example1

import java.net.InetSocketAddress
import java.nio.file.Paths
import com.treode.cluster.{CellId, ClusterConfig, HostId}
import com.treode.disk.{DiskConfig, DiskGeometry}
import com.treode.store.{Store, StoreConfig}

class Server extends AsyncFinatraServer {

  val superBlockBits = flag [Int] (
      "superBlockBits",
      14,
      "Size of the super block (log base 2)")

  val bindAddr = flag [InetSocketAddress] (
      "bind",
      "Address on which to listen for peers (default share)")

  val shareAddr = flag [InetSocketAddress] (
      "share",
      new InetSocketAddress (6278),
      "Address to share with peers")

  override def main() {

    if (args.length == 0) {
      println ("At least one path is required.")
      return
    }

    implicit val disksConfig = DiskConfig.suggested.copy (superBlockBits = superBlockBits())
    implicit val clusterConfig = ClusterConfig.suggested
    implicit val storeConfig = StoreConfig.suggested

    val controller = {
      val c = Store.recover (
          bindAddr = if (bindAddr.isDefined) bindAddr() else shareAddr(),
          shareAddr = shareAddr(),
          paths = args map (Paths.get (_)): _*)
      c.await()
    }

    register (new Resource (controller.store))
    register (new AdminAtlas (controller))

    super.main()
  }}
