package com.treode.store.simple

import com.treode.disk.PageDescriptor
import com.treode.pickle.Picklers

private trait TierPage

private object TierPage {

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
