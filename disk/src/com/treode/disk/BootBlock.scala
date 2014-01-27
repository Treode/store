package com.treode.disk

import java.nio.file.Path

private case class BootBlock (gen: Int, disks: Set [Path], roots: RootRegistry.Meta)

private object BootBlock {

  val pickler = {
    import DiskPicklers._
    wrap (int, set (path), roots)
    .build ((apply _).tupled)
    .inspect (v => (v.gen, v.disks, v.roots))
  }}
