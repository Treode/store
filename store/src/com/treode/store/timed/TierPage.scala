package com.treode.store.timed

import com.treode.disk.PageDescriptor
import com.treode.pickle.Picklers

trait TierPage

object TierPage {

  val pickler = {
    import Picklers._
    tagged [TierPage] (
      0x1 -> IndexPage.pickler,
      0x2 -> CellPage.pickler)
  }

  val page = {
    import Picklers._
    PageDescriptor (0x45CF6FFC, ulong, pickler)
  }}
