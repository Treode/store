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

import scala.language.postfixOps

import com.treode.async.Backoff
import com.treode.async.misc.RichInt
import com.treode.disk.stubs.StubDiskConfig

class StoreTestConfig (
    val messageFlakiness: Double
) (implicit
    val stubDiskConfig: StubDiskConfig,
    val storeConfig: StoreConfig
) {

  override def toString = {
    import stubDiskConfig._
    import storeConfig._
    val s = new StringBuilder
    s ++= s"checkpointProbability = $checkpointProbability, "
    s ++= s"compactionProbability = $compactionProbability, "
    s ++= s"closedLifetime = $closedLifetime, "
    s ++= s"deliberatingTimeout = $deliberatingTimeout, "
    s ++= s"exodusThreshold = $exodusThreshold, "
    s ++= s"falsePositiveProbability = $falsePositiveProbability, "
    s ++= s"lockSpaceBits = $lockSpaceBits, "
    s ++= s"messageFlakiness = $messageFlakiness, "
    s ++= s"moveBatchBackoff = $moveBatchBackoff, "
    s ++= s"moveBatchBytes = $moveBatchBytes, "
    s ++= s"moveBatchEntries = $moveBatchEntries, "
    s ++= s"prepareBackoff = $prepareBackoff, "
    s ++= s"preparingTimeout = $preparingTimeout, "
    s ++= s"proposingBackoff = $proposingBackoff, "
    s ++= s"readBackoff = $readBackoff, "
    s ++= s"retention = $retention, "
    s ++= s"scanBatchBackoff = $scanBatchBackoff, "
    s ++= s"targetPageBytes = $targetPageBytes)"
    s.result
  }}

object StoreTestConfig {

  def storeConfig (
      closedLifetime: Int = 2 seconds,
      deliberatingTimeout: Int = 2 seconds,
      exodusThreshold: Double = 0.2D,
      falsePositiveProbability: Double = 0.01,
      lockSpaceBits: Int = 4,
      messageFlakiness: Double = 0.1,
      moveBatchBackoff: Backoff = Backoff (2 seconds, 1 seconds, 1 minutes, 7),
      moveBatchBytes: Int = Int.MaxValue,
      moveBatchEntries: Int = Int.MaxValue,
      prepareBackoff: Backoff = Backoff (100, 100, 1 seconds, 7),
      preparingTimeout: Int = 5 seconds,
      proposingBackoff: Backoff = Backoff (100, 100, 1 seconds, 7),
      readBackoff: Backoff = Backoff (100, 100, 1 seconds, 7),
      retention: Retention = Retention.UnixEpoch,
      scanBatchBackoff: Backoff = Backoff (700, 300, 10 seconds, 7),
      targetPageBytes: Int = 1<<10
  ): StoreConfig =
    StoreConfig (
        closedLifetime = closedLifetime,
        deliberatingTimeout = deliberatingTimeout,
        exodusThreshold = exodusThreshold,
        falsePositiveProbability = falsePositiveProbability,
        lockSpaceBits = lockSpaceBits,
        moveBatchBackoff = moveBatchBackoff,
        moveBatchBytes = moveBatchBytes,
        moveBatchEntries = moveBatchEntries,
        prepareBackoff = prepareBackoff,
        preparingTimeout = preparingTimeout,
        proposingBackoff = proposingBackoff,
        readBackoff = readBackoff,
        retention = retention,
        scanBatchBackoff = scanBatchBackoff,
        targetPageBytes = targetPageBytes)

  def apply (
      checkpointProbability: Double = 0.1,
      compactionProbability: Double = 0.1,
      closedLifetime: Int = 2 seconds,
      deliberatingTimeout: Int = 2 seconds,
      exodusThreshold: Double = 0.2D,
      falsePositiveProbability: Double = 0.01,
      lockSpaceBits: Int = 4,
      messageFlakiness: Double = 0.1,
      moveBatchBackoff: Backoff = Backoff (2 seconds, 1 seconds, 1 minutes, 7),
      moveBatchBytes: Int = Int.MaxValue,
      moveBatchEntries: Int = Int.MaxValue,
      prepareBackoff: Backoff = Backoff (100, 100, 1 seconds, 7),
      preparingTimeout: Int = 5 seconds,
      proposingBackoff: Backoff = Backoff (100, 100, 1 seconds, 7),
      readBackoff: Backoff = Backoff (100, 100, 1 seconds, 7),
      retention: Retention = Retention.UnixEpoch,
      scanBatchBackoff: Backoff = Backoff (700, 300, 10 seconds, 7),
      targetPageBytes: Int = 1<<10
  ): StoreTestConfig = {
    new StoreTestConfig (
        messageFlakiness = messageFlakiness
    ) (
        stubDiskConfig = StubDiskConfig (
            checkpointProbability = checkpointProbability,
            compactionProbability = compactionProbability),
        storeConfig = StoreConfig (
            closedLifetime = closedLifetime,
            deliberatingTimeout = deliberatingTimeout,
            exodusThreshold = exodusThreshold,
            falsePositiveProbability = falsePositiveProbability,
            lockSpaceBits = lockSpaceBits,
            moveBatchBackoff = moveBatchBackoff,
            moveBatchBytes = moveBatchBytes,
            moveBatchEntries = moveBatchEntries,
            prepareBackoff = prepareBackoff,
            preparingTimeout = preparingTimeout,
            proposingBackoff = proposingBackoff,
            readBackoff = readBackoff,
            retention = retention,
            scanBatchBackoff = scanBatchBackoff,
            targetPageBytes = targetPageBytes))
  }}
