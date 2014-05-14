package com.treode.disk.stubs

private case class StubConfig (
    checkpointProbability: Double,
    compactionProbability: Double
) {

  require (0.0 <= checkpointProbability && checkpointProbability <= 1.0)
  require (0.0 <= compactionProbability && compactionProbability <= 1.0)
}
