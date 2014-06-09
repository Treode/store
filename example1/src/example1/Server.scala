package example1

import java.net.InetSocketAddress
import java.nio.file.Paths
import com.treode.cluster.{CellId, HostId}
import com.treode.disk.{DiskConfig, DiskGeometry}
import com.treode.store.{StandAlone, StoreConfig}

class Server extends AsyncFinatraServer {

  val superBlockBits = flag [Int] (
      "superBlockBits",
      14,
      "Size of the super block (log base 2)")

  val peerPort = flag [InetSocketAddress] (
      "peerPort",
      InetSocketAddress.createUnresolved ("*", 6278),
      "Port on which to listen for peers")

  override def main() {

    if (args.length == 0) {
      println ("At least one path is required.")
      return
    }

    implicit val disksConfig =
        DiskConfig.recommended (superBlockBits = superBlockBits())

    val controller = {
      val c = StandAlone.recover (
          localAddr = peerPort(),
          disksConfig = disksConfig,
          storeConfig = StoreConfig.recommended(),
          paths = args map (Paths.get (_)): _*)
      c.await()
    }

    register (new Resource (controller.store))
    register (new AdminAtlas (controller))

    super.main()
  }}
