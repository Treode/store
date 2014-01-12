package com.treode.store.disk2

import java.nio.file.{Path, Paths}
import com.treode.pickle.Picklers

private case class SuperBlock (
    id: Int,
    boot: BootBlock,
    config: DiskDriveConfig,
    alloc: Allocator.Meta,
    log: LogWriter.Meta,
    pages: PageWriter.Meta)

private object SuperBlock {

  val pickle = {
    import Picklers._
    val boot = BootBlock.pickle
    val config = DiskDriveConfig.pickle
    val alloc = Allocator.Meta.pickle
    val log = LogWriter.Meta.pickle
    val pages = PageWriter.Meta.pickle
    wrap (int, boot, config, alloc, log, pages)
    .build ((SuperBlock.apply _).tupled)
    .inspect (v => (v.id, v.boot, v.config, v.alloc, v.log, v.pages))
  }}
