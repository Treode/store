package com.treode.store.tier

import java.util.ArrayList

import com.treode.cluster.concurrent.Callback

private class DiskStub (val maxBlockSize: Int) extends BlockWriter with BlockCache {

  private val blocks = new ArrayList [Block] ()

  def write (block: Block, cb: Callback [Long]) {
    val pos = blocks.size
    blocks.add (block)
    cb (pos)
  }

  def write2 (block: Block): Long = {
    val pos = blocks.size
    blocks.add (block)
    pos
  }

  def get (pos: Long, cb: Callback [Block]): Unit = {
    require (pos < Int.MaxValue)
    cb (blocks.get (pos.toInt))
  }

  def get (pos: Long): Block = {
    require (pos < Int.MaxValue)
    blocks.get (pos.toInt)
  }

  /** Iterate the values of the tier rooted at `pos`. */
  def iterator (pos: Long): Iterator [ValueEntry] =
    new TierIteratorStub (this, pos)

  private def spaces (builder: StringBuilder, indent: Int) {
    for (i <- 0 until indent*4)
      builder.append (' ')
  }

  private def toTreeString (builder: StringBuilder, pos: Long, indent: Int) {
    blocks.get (pos.toInt) match {
      case block: ValueBlock =>
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
