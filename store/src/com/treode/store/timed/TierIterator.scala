package com.treode.store.timed

import com.treode.async.{AsyncIterator, Callback, callback}
import com.treode.disk.{Disks, Position}
import com.treode.store.TimedCell

private class TierIterator (implicit disks: Disks) extends AsyncIterator [TimedCell] {

  private var stack = List.empty [(IndexPage, Int)]
  private var page: CellPage = null
  private var index = 0

  private def find (pos: Position, cb: Callback [TierIterator]) {

    val loop = new Callback [TierPage] {

      def pass (p: TierPage) {
        p match {
          case p: IndexPage =>
            val e = p.get (0)
            stack ::= (p, 0)
            TierPage.page.read (e.pos, this)
          case p: CellPage =>
            page = p
            index = 0
            cb (TierIterator.this)
        }}

      def fail (t: Throwable) = cb.fail (t)
    }

    TierPage.page.read (pos, loop)
  }

  def hasNext: Boolean =
    index < page.size

  def next (cb: Callback [TimedCell]) {
    val entry = page.get (index)
    index += 1
    if (index == page.size && !stack.isEmpty) {
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
        find (b.get (i) .pos, callback (_ => cb (entry)))
      } else {
        cb (entry)
      }
    } else {
      cb (entry)
    }}}

private object TierIterator {

  def apply (pos: Position, cb: Callback [TierIterator]) (implicit disks: Disks): Unit =
    new TierIterator() .find (pos, cb)
}
