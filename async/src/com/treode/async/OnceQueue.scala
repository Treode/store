package com.treode.async

private class OnceQueue [A] {

  private var v: A = null .asInstanceOf [A]

  def set (v: A): Unit = synchronized {
    require (this.v == null)
    this.v = v
    notify()
  }

  def await(): A = synchronized {
    while (v == null)
      wait()
    v
  }}
