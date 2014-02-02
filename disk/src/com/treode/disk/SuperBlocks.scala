package com.treode.disk

import java.nio.file.Path
import com.treode.async.io.File

private class SuperBlocks (
    val path: Path,
    val file: File,
    val sb1: Option [SuperBlock],
    val sb2: Option [SuperBlock]) {

  def superb (useGen1: Boolean): SuperBlock =
    if (useGen1) sb1.get else sb2.get

  override def toString = s"SuperBlocks($path, $sb1, $sb2)"
}
