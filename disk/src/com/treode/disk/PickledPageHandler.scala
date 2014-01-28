package com.treode.disk

import com.treode.pickle.PicklerRegistry

private trait PickledPageHandler extends PicklerRegistry.Tagged {

  def tag: PicklerRegistry.Tagger
}

private object PickledPageHandler {

  def apply [G, P] (desc: PageDescriptor [G, P], group: G): PickledPageHandler =
    new PickledPageHandler {

      def tag = PicklerRegistry.tagger (desc.pgrp, desc.id.id, group)
    }}
