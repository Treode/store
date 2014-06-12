package com.treode.disk

import java.nio.file.Path
import com.treode.async.Async
import com.treode.async.io.File

private class ControllerAgent (kit: DiskKit, val disk: Disk) extends Disk.Controller  {

  def drives: Async [Seq [DriveDigest]] =
    kit.drives.digest

  def attach (items: DriveAttachment*): Async [Unit] =
    kit.drives.attach (items)

  def drain (items: Path*): Async [Unit] =
    kit.drives.drain (items)

  def shutdown(): Async [Unit] =
    kit.close()
}
