package com.treode.disk

private trait PickledPageHandler {

  def retag: TagRegistry.Tagger
}

private object PickledPageHandler {

  def apply [G, P] (desc: PageDescriptor [G, P], group: G): PickledPageHandler =
    new PickledPageHandler {

      def retag = TagRegistry.tagger (desc.pgrp, desc.id.id, group)
    }}
