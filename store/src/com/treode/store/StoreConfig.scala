package com.treode.store

import scala.language.postfixOps

import com.treode.async.Backoff
import com.treode.async.misc.RichInt

case class StoreConfig (
    closedLifetime: Int,
    deliberatingTimeout: Int,
    exodusThreshold: Double,
    falsePositiveProbability: Double,
    lockSpaceBits: Int,
    moveBatchBackoff: Backoff,
    moveBatchBytes: Int,
    moveBatchEntries: Int,
    prepareBackoff: Backoff,
    preparingTimeout: Int,
    priorValueEpoch: Epoch,
    proposingBackoff: Backoff,
    readBackoff: Backoff,
    scanBatchBackoff: Backoff,
    scanBatchBytes: Int,
    scanBatchEntries: Int,
    targetPageBytes: Int
) {

  require (
      closedLifetime > 0,
      "The closed lifetime must be more than 0 milliseconds.")

  require (
      deliberatingTimeout > 0,
      "The deliberating timeout must be more than 0 milliseconds.")

  require (
      0.0 < exodusThreshold && exodusThreshold < 1.0,
      "The exodus threshould must be between 0 and 1 exclusive.")

  require (
      0 < falsePositiveProbability && falsePositiveProbability < 1,
      "The false positive probability must be between 0 and 1 exclusive.")

  require (
      0 <= lockSpaceBits && lockSpaceBits <= 14,
      "The size of the lock space must be between 0 and 14 bits.")

  require (
      moveBatchBytes > 0,
      "The move batch size must be more than 0 bytes.")

  require (
      moveBatchEntries > 0,
      "The move batch size must be more than 0 entries.")

  require (
      preparingTimeout > 0,
      "The preparing timeout must be more than 0 milliseconds.")

  require (
      scanBatchBytes > 0,
      "The scan batch size must be more than 0 bytes.")

  require (
      scanBatchEntries > 0,
      "The scan batch size must be more than 0 entries.")

  require (
      targetPageBytes > 0,
      "The target size of a page must be more than zero bytes.")
}

object StoreConfig {

  val suggested = StoreConfig (
      closedLifetime = 2 seconds,
      deliberatingTimeout = 2 seconds,
      exodusThreshold = 0.2D,
      falsePositiveProbability = 0.01,
      lockSpaceBits = 10,
      moveBatchBackoff = Backoff (2 seconds, 1 seconds, 5 minutes),
      moveBatchBytes = 1<<20,
      moveBatchEntries = 10000,
      prepareBackoff = Backoff (100, 100, 1 seconds, 7),
      preparingTimeout = 5 seconds,
      priorValueEpoch = Epoch.StartOfYesterday,
      proposingBackoff = Backoff (100, 100, 1 seconds, 7),
      readBackoff = Backoff (100, 100, 1 seconds, 7),
      scanBatchBytes = 1<<16,
      scanBatchEntries = 1000,
      scanBatchBackoff = Backoff (700, 300, 10 seconds, 7),
      targetPageBytes = 1<<20)
}
