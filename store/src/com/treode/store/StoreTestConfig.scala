package com.treode.store

class StoreTestConfig (
    priorValueEpoch: Epoch,
    falsePositiveProbability: Double,
    lockSpaceBits: Int,
    targetPageBytes: Int,
    rebalanceBytes: Int,
    rebalanceEntries: Int
) extends StoreConfig (
    priorValueEpoch,
    falsePositiveProbability,
    lockSpaceBits,
    targetPageBytes,
    rebalanceBytes,
    rebalanceEntries
)

object StoreTestConfig {

  def apply (
      priorValueEpoch: Epoch = Epoch.zero,
      falsePositiveProbability: Double = 0.01,
      lockSpaceBits: Int = 4,
      targetPageBytes: Int = 1<<10,
      rebalanceBytes: Int = Int.MaxValue,
      rebalanceEntries: Int = Int.MaxValue
  ): StoreTestConfig =
    new StoreTestConfig (
        priorValueEpoch,
        falsePositiveProbability,
        lockSpaceBits,
        targetPageBytes,
        rebalanceBytes,
        rebalanceEntries)
}
