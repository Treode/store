package com.treode.store

import scala.language.postfixOps
import com.treode.async.Backoff
import com.treode.async.misc.RichInt

class StoreConfig private (
    val lockSpaceBits: Int,
    val targetPageBytes: Int,
    val rebalanceBytes: Int,
    val rebalanceEntries: Int
) {

  val deliberatingTimeout = 2 seconds
  val preparingTimeout = 5 seconds
  val readBackoff = Backoff (100, 100, 1 seconds, 7)
  val prepareBackoff = Backoff (100, 100, 1 seconds, 7)
  val closedLifetime = 2 seconds
}

object StoreConfig {

  def apply (
      lockSpaceBits: Int,
      targetPageBytes: Int,
      rebalanceBytes: Int,
      rebalanceEntries: Int
  ): StoreConfig = {

    require (
        0 <= lockSpaceBits && lockSpaceBits <= 14,
        "The size of the lock space must be between 0 and 14 bits.")
    require (
        targetPageBytes > 0,
        "The target size of a page must be more than zero bytes.")
    require (
        rebalanceBytes > 0,
        "The rebalance batch size must be more than 0 bytes.")
    require (
        rebalanceEntries > 0,
        "The rebalance batch size must be more than 0 entries.")

    new StoreConfig (
        lockSpaceBits,
        targetPageBytes,
        rebalanceBytes,
        rebalanceEntries)
  }

  def recommended (
      lockSpaceBits: Int = 10,
      targetPageBytes: Int = 1<<20,
      rebalanceBytes: Int = 1<<20,
      rebalanceEntries: Int = 10000
  ): StoreConfig =
    StoreConfig (
        lockSpaceBits,
        targetPageBytes,
        rebalanceBytes,
        rebalanceEntries)
}
