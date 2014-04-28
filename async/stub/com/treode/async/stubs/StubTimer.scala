package com.treode.async.stubs

private case class StubTimer (time: Long, task: Runnable) extends Ordered [StubTimer] {

  // Reverse the sort for the PriorityQueue.
  def compare (that: StubTimer) = that.time compare time
}

private object StubTimer extends Ordering [StubTimer] {

  def compare (x: StubTimer, y: StubTimer) = x compare y
}
