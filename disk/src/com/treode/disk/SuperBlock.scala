package com.treode.disk

private case class SuperBlock (
    id: Int,
    boot: BootBlock,
    config: DiskDriveConfig,
    alloc: SegmentAllocator.Meta,
    log: LogWriter.Meta,
    pages: PageWriter.Meta)

private object SuperBlock {

  val pickler = {
    import DiskPicklers._
    wrap (int, boot, config, allocMeta, logMeta, pageMeta)
    .build ((SuperBlock.apply _).tupled)
    .inspect (v => (v.id, v.boot, v.config, v.alloc, v.log, v.pages))
  }}
