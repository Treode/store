package com.treode.disk

import com.treode.async.Callback

private class SegmentCleaner {

  private val handlers = new TagRegistry [PickledPageHandler]

  def handle [G, P] (desc: PageDescriptor [G, P], handler: PageHandler [G]): Unit =
    handlers.register (desc.pgrp, desc.id.id) (PickledPageHandler (desc, _))
}

private object SegmentCleaner {


}
