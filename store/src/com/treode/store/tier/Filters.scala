package com.treode.store.tier

import com.treode.async.{AsyncIterator, Callback}
import com.treode.store.{Bytes, CellIterator, TxClock}

private object Filters {

  /** Preserves first cell for key and time and eliminates subsequent ones.  Expects the input
    * iterator to be sorted by key and time.
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
      }}}

  /** Keep all that are newer than the limit; keep only the one newest that's older than the
    * limit.  Expects the input iterator to be sorted by key and reverse sorted by time.
    */
  def retire (iter: CellIterator, limit: TxClock): CellIterator = {
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
