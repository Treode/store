/*
 * Copyright 2014 Treode, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.treode.store

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
    proposingBackoff: Backoff,
    readBackoff: Backoff,
    retention: Retention,
    scanBatchBackoff: Backoff,
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
      targetPageBytes > 0,
      "The target size of a page must be more than zero bytes.")
}

object StoreConfig {

  /** Suggested for intra-datacenter replication; we chose timeouts supposing a RTT of 1ms. */
  val suggested = StoreConfig (
      closedLifetime = 2.seconds,
      deliberatingTimeout = 2.seconds,
      exodusThreshold = 0.2D,
      falsePositiveProbability = 0.01,
      lockSpaceBits = 10,
      moveBatchBackoff = Backoff (2.seconds, 3.seconds, 1.minutes, 7),
      moveBatchBytes = 1 << 20,
      moveBatchEntries = 10000,
      prepareBackoff = Backoff (100, 100, 1.seconds, 7),
      preparingTimeout = 5.seconds,
      proposingBackoff = Backoff (100, 100, 1.seconds, 7),
      readBackoff = Backoff (100, 100, 1.seconds, 7),
      retention = Retention.StartOfYesterday,
      scanBatchBackoff = Backoff (2.seconds, 3.seconds, 1.minutes, 7),
      targetPageBytes = 1 << 20)

  /** Suggested for cross-datacenter replication; we chose timeouts supposing a RTT of 300ms. */
  val xdcr = StoreConfig (
      closedLifetime = 10.seconds,
      deliberatingTimeout = 10.seconds,
      exodusThreshold = 0.2D,
      falsePositiveProbability = 0.01,
      lockSpaceBits = 10,
      moveBatchBackoff = Backoff (10.seconds, 20.seconds, 1.minutes, 7),
      moveBatchBytes = 1 << 20,
      moveBatchEntries = 10000,
      prepareBackoff = Backoff (3.seconds, 4.seconds, 20.seconds, 7),
      preparingTimeout = 10.seconds,
      proposingBackoff = Backoff (3.seconds, 4.seconds, 20.seconds, 7),
      readBackoff = Backoff (3.seconds, 4.seconds, 20.seconds, 7),
      retention = Retention.StartOfYesterday,
      scanBatchBackoff = Backoff (10.seconds, 20.seconds, 1.minutes, 7),
      targetPageBytes = 1 << 20)
}
