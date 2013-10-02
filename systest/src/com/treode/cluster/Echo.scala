package com.treode.cluster

import com.treode.pickle.Picklers
import com.treode.cluster.concurrent.Fiber

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

    def outer (fiber: Fiber, i: Int) {
      fiber.execute {

        val mbx = _echo.open()
        val acks = Acknowledgements.settled (0, 1, 2)
        _echo ("Hello World") (acks, mbx)

        def inner() {
          mbx.receive { case (s, from) =>
            fiber.execute {
              acks += from
              if (acks.quorum) {
                mbx.close()
                if ((i + 1) % n == 0) {
                  val end = System.currentTimeMillis
                  val ms = (end - start) .toDouble / n.toDouble
                  val qps = n.toDouble / (end - start) .toDouble * 1000.0
                  println ("%8d: %10.3f ms, %10.0f qps" format (i, ms, qps))
                  start = System.currentTimeMillis
                }
                outer (fiber, i + 1)
              } else {
                inner()
              }}}}
        inner()
      }}

    if (host.localId == HostId (2)) {
      start = System.currentTimeMillis
      outer (new Fiber (host.scheduler), 0)
    }}}
