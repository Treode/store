package com.treode.store

import scala.language.postfixOps
import com.treode.cluster.misc.RichInt

case class StoreConfig (targetPageBytes: Int) {

  val deliberatingTimeout = 2 seconds

  val closedLifetime = 2 seconds
}
