package com.treode.store.disk2

import java.nio.file.{Path, Paths}
import com.treode.pickle.Picklers

case class SuperBlock (
    boot: BootBlock,
    config: DiskConfig,
    gen: Int)

object SuperBlock {

  val pickle = {
    import Picklers._
    val boot = BootBlock.pickle
    val config = DiskConfig.pickle
    wrap3 (boot, config, int) {
      SuperBlock.apply _
    } {
      v => (v.boot, v.config, v.gen)
    }}}
