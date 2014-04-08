package com.treode.store.tier

import com.treode.async.{AsyncIterator, Callback}
import com.treode.store.CellIterator

private object Filters {

  /** Preserves first cell for key and eliminates subsequent ones.  Expects the input iterator
    * to be sorted by key.
    */
  def dedupe (iter: CellIterator): CellIterator = {
    var prev: MemKey = null
    iter.filter { cell =>
      val key = MemKey (cell)
      if (key == prev) {
        false
      } else {
        prev = key
        true
      }}}}
