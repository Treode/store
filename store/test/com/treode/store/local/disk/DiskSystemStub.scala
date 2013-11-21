package com.treode.store.local.disk

import java.util.ArrayList
import scala.collection.mutable.Builder

import com.treode.async.Callback

private [store] class DiskSystemStub (val maxPageSize: Int) extends DiskSystem {

  private val pages = new ArrayList [Page] ()

  def write (page: Page, cb: Callback [Long]) {
    val pos = pages.size
    pages.add (page)
    cb (pos)
  }

  def read (pos: Long, cb: Callback [Page]): Unit = {
    require (pos < Int.MaxValue)
    cb (pages.get (pos.toInt))
  }

  def read (pos: Long): Page = {
    require (pos < Int.MaxValue)
    pages.get (pos.toInt)
  }}
