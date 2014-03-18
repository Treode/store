package com.treode.disk

import java.nio.file.Path

private case class BootBlock (
    gen: Int,
    number: Int,
    disks: Set [Path])

private object BootBlock {

  val pickler = {
    import DiskPicklers._
    wrap (uint, uint, set (path))
    .build ((apply _).tupled)
    .inspect (v => (v.gen, v.number, v.disks))
  }}
