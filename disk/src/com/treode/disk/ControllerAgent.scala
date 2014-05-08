package com.treode.disk

import java.nio.file.Path
import java.util.concurrent.ExecutorService

import com.treode.async.Async
import com.treode.async.io.File

private class ControllerAgent (kit: DisksKit, val disks: Disks) extends Disks.Controller  {
  import kit.{disks => drives}

  def _attach (items: (Path, File, DiskGeometry)*): Async [Unit] =
    drives.attach (items)

  def attach (exec: ExecutorService, items: (Path, DiskGeometry)*): Async [Unit] =
    drives.attach (exec, items)

  def drain (items: Path*): Async [Unit] =
    drives.drain (items)
}
