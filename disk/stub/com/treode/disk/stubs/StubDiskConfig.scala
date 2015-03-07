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
