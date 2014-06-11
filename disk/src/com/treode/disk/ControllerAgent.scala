package com.treode.disk

import java.nio.file.Path
import com.treode.async.Async
import com.treode.async.io.File

private class ControllerAgent (kit: DiskKit, val disks: Disk) extends Disk.Controller  {
  import kit.{disks => drives}

  def attach (items: (Path, DiskGeometry)*): Async [Unit] =
    drives.attach (items)

  def drain (items: Path*): Async [Unit] =
    drives.drain (items)

  def shutdown(): Async [Unit] =
    kit.close()
}
