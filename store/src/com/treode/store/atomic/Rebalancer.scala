package com.treode.store.atomic

import com.treode.async.Async
import com.treode.store.Cohort

import Async.supply

private class Rebalancer (kit: AtomicKit) {
  import kit.atlas

  def rebalance (cohorts: Array [Cohort]): Async [Unit] =
    supply()

  def attach () {
    atlas.rebalance (rebalance _)
  }}
