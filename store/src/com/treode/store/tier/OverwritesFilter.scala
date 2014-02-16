package com.treode.store.tier

import com.treode.async.{AsyncIterator, Callback}

/** Preserves first cell for key and eliminates subsequent ones. */
object OverwritesFilter {

  def apply (iter: CellIterator): CellIterator = {
    var prev: Cell = null
    iter.filter { cell =>
      if (cell == prev) {
        false
      } else {
        prev = cell
        true
      }}}
}
