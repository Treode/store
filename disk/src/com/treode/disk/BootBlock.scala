package com.treode.disk

import java.nio.file.Path

private case class BootBlock (
    cell: CellId,
    gen: Int,
    number: Int,
    disks: Set [Path])

private object BootBlock {

  val pickler = {
    import DiskPicklers._
    wrap (cellId, uint, uint, set (path))
    .build ((apply _).tupled)
    .inspect (v => (v.cell, v.gen, v.number, v.disks))
  }}
