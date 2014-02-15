package com.treode.store.simple

import com.treode.async.{AsyncIterator, Callback, callback}

private class TestIterator (iter: SimpleIterator) extends AsyncIterator [TestCell] {

  def hasNext =
    iter.hasNext

  def next (cb: Callback [TestCell]) =
    iter.next (callback (cb) (new TestCell (_)))
}
