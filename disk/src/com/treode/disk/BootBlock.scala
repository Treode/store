package com.treode.disk

import java.nio.file.Path

private case class BootBlock (gen: Int, num: Int, disks: Set [Path], roots: Position)

private object BootBlock {

  val pickler = {
    import DiskPicklers._
    wrap (int, int, set (path), pos)
    .build ((apply _).tupled)
    .inspect (v => (v.gen, v.num, v.disks, v.roots))
  }}
