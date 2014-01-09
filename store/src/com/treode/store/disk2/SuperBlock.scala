package com.treode.store.disk2

import java.nio.file.{Path, Paths}
import com.treode.pickle.Picklers

private case class SuperBlock (
    boot: BootBlock,
    config: DiskConfig,
    free: Allocator.Meta,
    log: LogWriter.Meta)

private object SuperBlock {

  val pickle = {
    import Picklers._
    val boot = BootBlock.pickle
    val config = DiskConfig.pickle
    val free = Allocator.Meta.pickle
    val log = LogWriter.Meta.pickle
    wrap4 (boot, config, free, log) {
      SuperBlock.apply _
    } {
      v => (v.boot, v.config, v.free, v.log)
    }}}
