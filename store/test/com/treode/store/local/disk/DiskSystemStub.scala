package com.treode.store.local.disk

import java.util.ArrayList
import scala.collection.mutable.Builder

import com.treode.async.Callback
import com.treode.store.disk2.Position

private [store] class DiskSystemStub (val maxPageSize: Int) extends DiskSystem {

  private val pages = new ArrayList [Page] ()

  def write (page: Page, cb: Callback [Position]) {
    val offset = pages.size
    pages.add (page)
    cb (Position (0, offset, 0))
  }

  def read (pos: Position, cb: Callback [Page]): Unit =
    cb (pages.get (pos.offset.toInt))

  def read (pos: Position): Page =
    pages.get (pos.offset.toInt)
}
