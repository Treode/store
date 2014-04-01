package com.treode.store.tier

import com.treode.async.{AsyncIterator, Callback}

private object Filters {

  /** Preserves first cell for key and eliminates subsequent ones.  Expects the input iterator
    * to be sorted by key.
    */
  def dedupe (iter: TierCellIterator): TierCellIterator = {
    var prev: TierCell = null
    iter.filter { cell =>
      if (cell == prev) {
        false
      } else {
        prev = cell
        true
      }}}}
