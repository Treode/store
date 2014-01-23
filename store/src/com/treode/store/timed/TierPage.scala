package com.treode.store.timed

import com.treode.disk.PageDescriptor
import com.treode.pickle.Picklers

trait TierPage

object TierPage {

  val pickle = {
    import Picklers._
    tagged [TierPage] (
      0x1 -> IndexPage.pickle,
      0x2 -> CellPage.pickle)
  }

  val page = {
    import Picklers._
    new PageDescriptor (0x45CF6FFC, const (0), pickle)
  }}
