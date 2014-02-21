package com.treode.async

object Latch {

  def map [K, V] (count: Int, cb: Callback [Map [K, V]]): Callback [(K, V)] =
    new MapLatch (count, cb)

  def indexed [A] (count: Int, cb: Callback [Seq [A]]) (implicit m: Manifest [A]): Callback [(Int, A)] =
    new IndexedLatch (count, cb)

  def seq [A] (count: Int, cb: Callback [Seq [A]]) (implicit m: Manifest [A]): Callback [A] =
    new SeqLatch (count, cb)

  def pair [A, B] (cb: Callback [(A, B)]): (Callback [A], Callback [B]) = {
    val t = new PairLatch (cb)
    (t.cbA, t.cbB)
  }

  def triple [A, B, C] (cb: Callback [(A, B, C)]): (Callback [A], Callback [B], Callback [C]) = {
    val t = new TripleLatch (cb)
    (t.cbA, t.cbB, t.cbC)
  }

  def unit [A] (count: Int, cb: Callback [Unit]): Callback [A] =
    new CountingLatch [A] (count, cb)
}
