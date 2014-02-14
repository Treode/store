package com.treode.disk

import java.nio.file.Path

private case class BootBlock (
    bootgen: Int,
    number: Int,
    disks: Set [Path],
    rootgen: Int,
    roots: Position)

private object BootBlock {

  val pickler = {
    import DiskPicklers._
    wrap (uint, uint, set (path), uint, pos)
    .build ((apply _).tupled)
    .inspect (v => (v.bootgen, v.number, v.disks, v.rootgen, v.roots))
  }}
