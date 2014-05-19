package com.treode.disk.stubs

import scala.util.Random

class StubDiskConfig private (
    val checkpointProbability: Double,
    val compactionProbability: Double
) {

  val checkpointEntries =
    if (checkpointProbability > 0)
      (2 / checkpointProbability).toInt
    else
      Int.MaxValue

  val compactionEntries =
    if (compactionProbability > 0)
      (2 / compactionProbability).toInt
    else
      Int.MaxValue

  def checkpoint (entries: Int) (implicit random: Random): Boolean =
    checkpointProbability > 0.0 &&
      (entries > checkpointEntries || random.nextDouble < checkpointProbability)

  def compact (entries: Int) (implicit random: Random): Boolean =
    compactionProbability > 0.0 &&
      (entries > compactionEntries || random.nextDouble < compactionProbability)
}

object StubDiskConfig {

  def apply (
      checkpointProbability: Double = 0.1,
      compactionProbability: Double = 0.1
  ): StubDiskConfig = {

      require (0.0 <= checkpointProbability && checkpointProbability <= 1.0)

      require (0.0 <= compactionProbability && compactionProbability <= 1.0)

      new StubDiskConfig (
          checkpointProbability,
          compactionProbability)
  }}
