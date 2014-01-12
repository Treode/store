package com.treode.store.disk2

trait Replay {

  def time: Long
  def replay()
}

private object Replay {

  def noop (_time: Long): Replay =
    new Replay {
      def time = _time
      def replay(): Unit = ()
      override def toString = "Replay.noop"
    }

  def apply [A] (_time: Long, id: TypeId, f: A => Any, v: A): Replay =
    new Replay {
      def time = _time
      def replay(): Unit = f (v)
      override def toString = s"Replay($time, $id, $v)"
  }}
