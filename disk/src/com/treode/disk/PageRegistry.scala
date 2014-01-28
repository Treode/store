package com.treode.disk

import com.treode.pickle.PicklerRegistry

private class PageRegistry {

  private val pages = PicklerRegistry [PickledPageHandler] ()

  def handle [G, P] (desc: PageDescriptor [G, P], handle: PageHandler [G]): Unit =
    pages.register (desc.pgrp, desc.id.id) (PickledPageHandler (desc, _))

  def pickler = pages.pickler
}
