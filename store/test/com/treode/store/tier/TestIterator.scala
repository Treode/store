package com.treode.store.tier

import com.treode.async.{AsyncIterator, Callback, callback}

private class TestIterator (iter: CellIterator) extends AsyncIterator [TestCell] {

  def hasNext =
    iter.hasNext

  def next (cb: Callback [TestCell]) =
    iter.next (callback (cb) (new TestCell (_)))
}
