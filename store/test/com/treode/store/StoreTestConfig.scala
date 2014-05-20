package com.treode.store

import com.treode.disk.stubs.StubDiskConfig

class StoreTestConfig (
    priorValueEpoch: Epoch,
    falsePositiveProbability: Double,
    lockSpaceBits: Int,
    targetPageBytes: Int,
    rebalanceBytes: Int,
    rebalanceEntries: Int,
    val checkpointProbability: Double,
    val compactionProbability: Double,
    val messageFlakiness: Double
) extends StoreConfig (
    priorValueEpoch,
    falsePositiveProbability,
    lockSpaceBits,
    targetPageBytes,
    rebalanceBytes,
    rebalanceEntries
) {

  val stubDiskConfig = StubDiskConfig (
      checkpointProbability,
      compactionProbability)

  override def toString = {
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
      priorValueEpoch: Epoch = Epoch.zero,
      falsePositiveProbability: Double = 0.01,
      lockSpaceBits: Int = 4,
      targetPageBytes: Int = 1<<10,
      rebalanceBytes: Int = Int.MaxValue,
      rebalanceEntries: Int = Int.MaxValue,
      checkpointProbability: Double = 0.1,
      compactionProbability: Double = 0.1,
      messageFlakiness: Double = 0.1
  ): StoreTestConfig =
    new StoreTestConfig (
        priorValueEpoch,
        falsePositiveProbability,
        lockSpaceBits,
        targetPageBytes,
        rebalanceBytes,
        rebalanceEntries,
        checkpointProbability,
        compactionProbability,
        messageFlakiness)
}
