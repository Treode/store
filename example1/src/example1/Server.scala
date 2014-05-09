package example1

import java.net.InetSocketAddress
import java.nio.file.Paths
import com.treode.disk.{DisksConfig, DiskGeometry}
import com.treode.store.{StandAlone, StoreConfig}

object Server extends AsyncFinatraServer {

  implicit val disksConfig =
    DisksConfig.recommended (0x7D7A5F10A567B675L)

  val controller = {
    val c = StandAlone.create (
        localId = 0x288ACE6509E0CA47L,
        localAddr = InetSocketAddress.createUnresolved ("*", 6782),
        disksConfig = disksConfig,
        storeConfig = StoreConfig.recommended(),
        items = Paths.get ("store.db") -> DiskGeometry (28, 13, 1L<<38))
    c.await()
  }

  register (new Resource (controller.store))
}
