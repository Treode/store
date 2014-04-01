package com.treode.store.atomic

import com.treode.store.{Bytes, CellIterator}

private object TimedFilters {

  /** Keep all that are newer than the limit; keep only the one newest that's older than the
    * limit.  Expects the input iterator to be sorted by key and reverse sorted by time.
    */
  def clean (iter: CellIterator, limit: Long): CellIterator = {
    var key = Option.empty [Bytes]
    iter.filter { cell =>
      if (cell.time >= limit) {
        key = None
        true
      } else if (key.isEmpty || cell.key != key.get) {
        key = Some (cell.key)
        true
      } else {
        false
      }}}}
