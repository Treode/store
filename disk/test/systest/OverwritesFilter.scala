package systest

import com.treode.async.{AsyncIterator, Callback}

/** Preserves first cell for key and eliminates subsequent ones. */
class OverwritesFilter private extends (Cell => Boolean) {

  private var prev: Cell = null

  def apply (cell: Cell): Boolean = {
    if (cell == prev) {
      false
    } else {
      prev = cell
      true
    }}}

object OverwritesFilter {

  def apply (iter: AsyncIterator [Cell], cb: Callback [AsyncIterator [Cell]]): Unit =
    AsyncIterator.filter (iter, cb) (new OverwritesFilter)
}
