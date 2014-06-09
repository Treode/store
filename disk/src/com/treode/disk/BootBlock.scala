package com.treode.disk

import java.nio.file.Path

private case class BootBlock (
    sysid: Array [Byte],
    gen: Int,
    number: Int,
    disks: Set [Path])

private object BootBlock {

  val pickler = {
    import DiskPicklers._
    wrap (array (byte), uint, uint, set (path))
    .build ((apply _).tupled)
    .inspect (v => (v.sysid, v.gen, v.number, v.disks))
  }}
