package com.treode.store.disk2

import java.nio.file.{Path, Paths}
import com.treode.pickle.Picklers

case class BootBlock (gen: Int, disks: Set [Path])

object BootBlock {

  val pickle = {
    import Picklers._
    val path = wrap1 (string) (Paths.get (_)) (_.toString)
    wrap2 (int, set (path)) (BootBlock.apply _) (v => (v.gen, v.disks))
  }}
