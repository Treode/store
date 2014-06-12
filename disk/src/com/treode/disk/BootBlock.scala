package com.treode.disk

import java.lang.{Integer => JInt}
import java.nio.file.Path
import java.util.{Arrays, Objects}

private case class BootBlock (
    sysid: Array [Byte],
    gen: Int,
    number: Int,
    drives: Set [Path]) {

  override def hashCode: Int =
    Objects.hash (Arrays.hashCode (sysid): JInt, gen: JInt, number: JInt, drives)

  override def equals (other: Any): Boolean =
    other match {
      case that: BootBlock =>
        Arrays.equals (sysid, that.sysid) &&
        gen == that.gen &&
        number == that.number &&
        drives == that.drives
      case _ => false
    }}

private object BootBlock {

  val pickler = {
    import DiskPicklers._
    wrap (array (byte), uint, uint, set (path))
    .build ((apply _).tupled)
    .inspect (v => (v.sysid, v.gen, v.number, v.drives))
  }}
