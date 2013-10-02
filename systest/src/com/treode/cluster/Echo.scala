package com.treode.cluster

import com.treode.pickle.Picklers

object Echo {

  private val _echo = {
    import Picklers._
    new MessageDescriptor (0xFF9F76CB490BE8A8L, int)
  }

  def attach () (implicit host: Host) {

    val n = 10000
    var start = System.currentTimeMillis;

    _echo.register (host.mailboxes) { case (i, from) =>
      if (i % n == 0) {
        val end = System.currentTimeMillis
        val ms = (end - start) .toDouble / n.toDouble
        val qps = n.toDouble / (end - start) .toDouble * 1000.0
        println ("%8d: %10.3f ms, %10.0f qps" format (i, ms, qps))
        start = System.currentTimeMillis
      }
      _echo (i+1) (from)
    }

    if (host.localId == HostId (1)) {
      _echo (0) (0)
    }}}
