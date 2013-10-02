package com.treode.cluster

import com.treode.pickle.Picklers

object Echo {

  private val _echo = {
    import Picklers._
    new RequestDescriptor (0xFF9F76CB490BE8A8L, string, string)
  }

  def attach () (implicit host: Host) {

    val n = 10000
    var start = 0L

    _echo.register { case (s, mdtr) =>
      mdtr.respond (s)
    }

    def loop (i: Int) {
      val mbx = _echo.open()
      _echo.apply ("Hello World") .apply (0L, mbx)
      mbx.receive { case (s, from) =>
        if ((i + 1) % n == 0) {
          val end = System.currentTimeMillis
          val ms = (end - start) .toDouble / n.toDouble
          val qps = n.toDouble / (end - start) .toDouble * 1000.0
          println ("%8d: %10.3f ms, %10.0f qps" format (i, ms, qps))
          start = System.currentTimeMillis
        }
        loop (i + 1)
      }}

    if (host.localId == HostId (1)) {
      start = System.currentTimeMillis
      loop (0)
    }}}
