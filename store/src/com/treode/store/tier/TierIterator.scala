package com.treode.store.tier

import com.treode.cluster.concurrent.Callback
import com.treode.store.log.{Block, BlockCache}

private class TierIterator (cache: BlockCache) extends CellIterator {

  private var stack = List.empty [(IndexBlock, Int)]
  private var block: CellBlock = null
  private var index = 0

  private def find (pos: Long, cb: Callback [Unit]) {

    val loop = new Callback [Block] {

      def apply (b: Block) {
        b match {
          case b: IndexBlock =>
            val e = b.get (0)
            stack ::= (b, 0)
            cache.get (e.pos, this)
          case b: CellBlock =>
            block = b
            index = 0
            cb()
        }}

      def fail (t: Throwable) = cb.fail (t)
    }

    cache.get (pos, loop)
  }

  def hasNext: Boolean =
    index < block.size

  def next (cb: Callback [Cell]) {
    val entry = block.get (index)
    index += 1
    if (index == block.size && !stack.isEmpty) {
      var b = stack.head._1
      var i = stack.head._2 + 1
      stack = stack.tail
      while (i == b.size && !stack.isEmpty) {
        b = stack.head._1
        i = stack.head._2 + 1
        stack = stack.tail
      }
      if (i < b.size) {
        stack ::= (b, i)
        find (b.get (i) .pos, new Callback [Unit] {
          def apply (v: Unit): Unit = cb (entry)
          def fail (t: Throwable) = cb.fail (t)
        })
      } else {
        cb (entry)
      }
    } else {
      cb (entry)
    }}}

private object TierIterator {

  def apply (cache: BlockCache, pos: Long, cb: Callback [TierIterator]) {
    val iter = new TierIterator (cache)
    iter.find (pos, new Callback [Unit] {
      def apply (v: Unit): Unit = cb (iter)
      def fail (t: Throwable) = cb.fail (t)
    })
  }}
