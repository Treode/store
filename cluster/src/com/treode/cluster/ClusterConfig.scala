package com.treode.cluster

import scala.language.postfixOps

import com.treode.async.Backoff
import com.treode.async.misc.RichInt

case class ClusterConfig (
    connectingBackoff: Backoff,
    statsUpdatePeriod: Int
) {

  require (
      statsUpdatePeriod > 0,
      "The stats update period must be more than 0 milliseconds.")
}

object ClusterConfig {

  val suggested = ClusterConfig (
      connectingBackoff = Backoff (3 seconds, 2 seconds, 3 minutes),
      statsUpdatePeriod = 1 minutes)
}
