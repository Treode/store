package com.treode.disk

import com.treode.pickle.PicklerRegistry

import PicklerRegistry.{BaseTag, Tag}

private trait PickledPageHandler extends Tag

private object PickledPageHandler {

  def apply [G, P] (desc: PageDescriptor [G, P], group: G): PickledPageHandler =
    new BaseTag (desc.pgrp, desc.id.id, group) with PickledPageHandler
}
