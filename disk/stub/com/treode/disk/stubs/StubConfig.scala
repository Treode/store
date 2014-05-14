package com.treode.disk.stubs

private case class StubConfig (checkpointProbability: Double) {

  require (0.0 <= checkpointProbability && checkpointProbability <= 1.0)
}
