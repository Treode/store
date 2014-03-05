package com.treode.store

import scala.language.postfixOps
import com.treode.async.misc.RichInt

class StoreConfig private (
    val lockSpaceBits: Int,
    val targetPageBytes: Int) {

  val deliberatingTimeout = 2 seconds

  val closedLifetime = 2 seconds
}

object StoreConfig {

  def apply (
      lockSpaceBits: Int,
      targetPageBytes: Int
  ): StoreConfig = {

    require (
        0 <= lockSpaceBits && lockSpaceBits <= 14,
        "The size of the lock space must be between 0 and 14 bits.")

    require (targetPageBytes > 0, "The target size of a page must be more than zero bytes.")

    new StoreConfig (lockSpaceBits, targetPageBytes)
  }
}
