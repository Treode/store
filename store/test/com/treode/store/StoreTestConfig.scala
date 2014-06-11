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
    s ++= "StoreTestConfig("
    s ++= s"priorValueEpoch = $priorValueEpoch, "
    s ++= s"falsePositiveProbability = $falsePositiveProbability, "
    s ++= s"lockSpaceBits = $lockSpaceBits, "
    s ++= s"targetPageBytes = $targetPageBytes, "
    s ++= s"rebalanceBytes = $rebalanceBytes, "
    s ++= s"rebalanceEntries = $rebalanceEntries, "
    s ++= s"checkpointProbability = $checkpointProbability, "
    s ++= s"compactionProbability = $compactionProbability, "
    s ++= s"messageFlakiness = $messageFlakiness"
    s ++= ")"
    s.result
  }}

object StoreTestConfig {

  def apply (
      priorValueEpoch: Epoch = Epoch.UnixEpoch,
      falsePositiveProbability: Double = 0.01,
      lockSpaceBits: Int = 4,
      targetPageBytes: Int = 1<<10,
      rebalanceBytes: Int = Int.MaxValue,
      rebalanceEntries: Int = Int.MaxValue,
      checkpointProbability: Double = 0.1,
      compactionProbability: Double = 0.1,
      messageFlakiness: Double = 0.1
  ): StoreTestConfig = {
    new StoreTestConfig (
        messageFlakiness = messageFlakiness
    ) (
        stubDiskConfig = StubDiskConfig (
            checkpointProbability = checkpointProbability,
            compactionProbability = compactionProbability),
        storeConfig = StoreConfig (
            priorValueEpoch = priorValueEpoch,
            falsePositiveProbability = falsePositiveProbability,
            lockSpaceBits = lockSpaceBits,
            targetPageBytes = targetPageBytes,
            rebalanceBytes = rebalanceBytes,
            rebalanceEntries = rebalanceEntries))
  }}
