package com.treode.concurrent

private case class Timer (time: Long, task: Runnable) extends Ordered [Timer] {

  def compare (that: Timer) = this.time compare that.time
}

private object Timer extends Ordering [Timer] {

  def compare (x: Timer, y: Timer) = x compare y
}
