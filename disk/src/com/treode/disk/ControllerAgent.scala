package com.treode.disk

import java.nio.file.Path
import java.util.concurrent.ExecutorService

import com.treode.async.Async
import com.treode.async.io.File

private class ControllerAgent (kit: DisksKit, val disks: Disks) extends Disks.Controller  {
  import kit.{disks => drives}

  def attach (items: Seq [(Path, File, DiskGeometry)]): Async [Unit] =
    drives.attach (items)

  def attach (items: Seq [(Path, DiskGeometry)], exec: ExecutorService): Async [Unit] =
    drives.attach (items, exec)

  def drain (items: Seq [Path]): Async [Unit] =
    drives.drain (items)
}
