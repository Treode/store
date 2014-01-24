package com.treode.disk

private case class SuperBlock (
    id: Int,
    boot: BootBlock,
    config: DiskDriveConfig,
    alloc: Allocator.Meta,
    log: LogWriter.Meta,
    pages: PageWriter.Meta)

private object SuperBlock {

  val pickle = {
    import DiskPicklers._
    wrap (int, boot, config, alloc, log, pages)
    .build ((SuperBlock.apply _).tupled)
    .inspect (v => (v.id, v.boot, v.config, v.alloc, v.log, v.pages))
  }}
