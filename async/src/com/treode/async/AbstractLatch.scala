package com.treode.async

import AsyncConversions._

private abstract class AbstractLatch [A] (private var count: Int, cb: Callback [A]) {

  private var thrown = List.empty [Throwable]

  def value: A

  def init() {
    if (count == 0)
      cb.pass (value)
  }

  def release() {
    require (count > 0, "Latch was already released.")
    count -= 1
    if (count > 0)
      return
    if (!thrown.isEmpty)
      cb.fail (MultiException.fit (thrown))
    else
      cb.pass (value)
  }

  def failure (t: Throwable) {
    thrown ::= t
    release()
  }}
