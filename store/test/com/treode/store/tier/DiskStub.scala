package com.treode.store.tier

import java.util.ArrayList
import scala.collection.mutable.Builder

import com.treode.cluster.concurrent.Callback
import com.treode.store.Cell

private class DiskStub (val maxBlockSize: Int) extends BlockWriter with BlockCache {

  private val blocks = new ArrayList [Block] ()

  def write (block: Block, cb: Callback [Long]) {
    val pos = blocks.size
    blocks.add (block)
    cb (pos)
  }

  def get (pos: Long, cb: Callback [Block]): Unit = {
    require (pos < Int.MaxValue)
    cb (blocks.get (pos.toInt))
  }

  def get (pos: Long): Block = {
    require (pos < Int.MaxValue)
    blocks.get (pos.toInt)
  }

  private def toSeq (builder: Builder [Cell, _], pos: Long) {
    get (pos) match {
      case block: IndexBlock =>
        block.entries foreach (e => toSeq (builder, e.pos))
      case block: CellBlock =>
        block.entries foreach (builder += _)
    }}

  /** Iterate the values of the tier rooted at `pos`. */
  def toSeq (pos: Long): Seq [Cell] = {
    val builder = Seq.newBuilder [Cell]
    toSeq (builder, pos)
    builder.result
  }

  private def spaces (builder: StringBuilder, indent: Int) {
    for (i <- 0 until indent*4)
      builder.append (' ')
  }

  private def toTreeString (builder: StringBuilder, pos: Long, indent: Int) {
    blocks.get (pos.toInt) match {
      case block: CellBlock =>
        spaces (builder, indent)
        builder.append ("ValueBlock(\n")
        val i = block.entries.iterator
        for (e <- i) {
          spaces (builder, indent+1)
          builder.append (e.key.bytes.map (_.toChar) .mkString (""))
          builder.append (if (i.hasNext) ",\n" else ")")
        }
      case block: IndexBlock =>
        spaces (builder, indent)
        builder.append ("IndexBlock(\n")
        val i = block.entries.iterator
        for (e <- i) {
          toTreeString (builder, e.pos, indent+1)
          builder.append (if (i.hasNext) ",\n" else ")")
        }}}

  /** Print the tree rooted at `pos`; suitable for small trees only. */
  def toTreeString (pos: Long): String = {
    val b = new StringBuilder
    toTreeString (b, pos, 0)
    b.result
  }}
