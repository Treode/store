package com.treode.store

import scala.language.postfixOps

import com.treode.async.Backoff
import com.treode.async.misc.RichInt
import com.treode.disk.stubs.StubDisk

class StoreTestConfig (
    val messageFlakiness: Double
) (implicit
    val stubDiskConfig: StubDisk.Config,
    val storeConfig: Store.Config
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
    s ++= s"priorValueEpoch = $priorValueEpoch, "
    s ++= s"proposingBackoff = $proposingBackoff, "
    s ++= s"readBackoff = $readBackoff, "
    s ++= s"scanBatchBackoff = $scanBatchBackoff, "
    s ++= s"scanBatchBytes = $scanBatchBytes, "
    s ++= s"scanBatchEntries = $scanBatchEntries, "
    s ++= s"targetPageBytes = $targetPageBytes)"
    s.result
  }}

object StoreTestConfig {

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
      priorValueEpoch: Epoch = Epoch.UnixEpoch,
      proposingBackoff: Backoff = Backoff (100, 100, 1 seconds, 7),
      readBackoff: Backoff = Backoff (100, 100, 1 seconds, 7),
      scanBatchBackoff: Backoff = Backoff (700, 300, 10 seconds, 7),
      scanBatchBytes: Int = 1<<16,
      scanBatchEntries: Int = 1000,
      targetPageBytes: Int = 1<<10
  ): StoreTestConfig = {
    new StoreTestConfig (
        messageFlakiness = messageFlakiness
    ) (
        stubDiskConfig = StubDisk.Config (
            checkpointProbability = checkpointProbability,
            compactionProbability = compactionProbability),
        storeConfig = Store.Config (
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
            priorValueEpoch = priorValueEpoch,
            proposingBackoff = proposingBackoff,
            readBackoff = readBackoff,
            scanBatchBackoff = scanBatchBackoff,
            scanBatchBytes = scanBatchBytes,
            scanBatchEntries = scanBatchEntries,
            targetPageBytes = targetPageBytes))
  }}
