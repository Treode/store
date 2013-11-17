package com.treode.concurrent

private case class StubTimer (time: Long, task: Runnable) extends Ordered [StubTimer] {

  def compare (that: StubTimer) = this.time compare that.time
}

private object StubTimer extends Ordering [StubTimer] {

  def compare (x: StubTimer, y: StubTimer) = x compare y
}
