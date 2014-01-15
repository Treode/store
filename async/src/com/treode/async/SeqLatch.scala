package com.treode.async

private class SeqLatch [A] (count: Int, cb: Callback [Seq [A]]) (implicit manifest: Manifest [A])
extends AbstractLatch [Seq [A]] (count, cb) with Callback [A] {

  private var values = Seq.newBuilder [A]
  values.sizeHint (count)

  init()

  def value = values.result

  def pass (v: A): Unit = synchronized {
    values += v
    release()
  }}
