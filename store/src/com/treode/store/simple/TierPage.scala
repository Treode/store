package com.treode.store.simple

import com.treode.disk.PageDescriptor
import com.treode.pickle.Picklers

private trait TierPage

private object TierPage {

  val pickler = {
    import Picklers._
    tagged [TierPage] (
      0x1 -> IndexPage.pickler,
      0x2 -> CellPage.pickler)
  }

  val page = {
    import Picklers._
    new PageDescriptor (0x45CF6FFC, const (0), pickler)
  }}
