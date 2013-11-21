package com.treode.store.local.disk.simple

import com.treode.async.Callback
import com.treode.store.SimpleCell
import com.treode.store.local.SimpleIterator
import com.treode.store.local.disk.{DiskSystem, Page}

private class TierIterator (disk: DiskSystem) extends SimpleIterator {

  private var stack = List.empty [(IndexPage, Int)]
  private var page: CellPage = null
  private var index = 0

  private def find (pos: Long, cb: Callback [Unit]) {

    val loop = new Callback [Page] {

      def pass (p: Page) {
        p match {
          case p: IndexPage =>
            val e = p.get (0)
            stack ::= (p, 0)
            disk.read (e.pos, this)
          case p: CellPage =>
            page = p
            index = 0
            cb()
        }}

      def fail (t: Throwable) = cb.fail (t)
    }

    disk.read (pos, loop)
  }

  def hasNext: Boolean =
    index < page.size

  def next (cb: Callback [SimpleCell]) {
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
        find (b.get (i) .pos, new Callback [Unit] {
          def pass (v: Unit): Unit = cb (entry)
          def fail (t: Throwable) = cb.fail (t)
        })
      } else {
        cb (entry)
      }
    } else {
      cb (entry)
    }}}

private object TierIterator {

  def apply (disk: DiskSystem, pos: Long, cb: Callback [TierIterator]) {
    val iter = new TierIterator (disk)
    iter.find (pos, new Callback [Unit] {
      def pass (v: Unit): Unit = cb (iter)
      def fail (t: Throwable) = cb.fail (t)
    })
  }}
