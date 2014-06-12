package com.treode.disk

import java.nio.file.Path
import com.treode.async.Async
import com.treode.async.io.File

private class ControllerAgent (kit: DiskKit, val disk: Disk) extends Disk.Controller  {
  import kit.drives

  def attach (items: (Path, DiskGeometry)*): Async [Unit] =
    drives.attach (items)

  def drain (items: Path*): Async [Unit] =
    drives.drain (items)

  def shutdown(): Async [Unit] =
    kit.close()
}
