package com.treode.store.simple

import com.treode.disk.{PageDescriptor, TypeId}
import com.treode.pickle.Picklers

private trait TierPage

private object TierPage {

  type Descriptor = PageDescriptor [Long, TierPage]

  val pickler = {
    import Picklers._
    tagged [TierPage] (
      0x1 -> IndexPage.pickler,
      0x2 -> CellPage.pickler)
  }

  def pager (id: TypeId) = {
    import Picklers._
    PageDescriptor (id, ulong, pickler)
  }}
