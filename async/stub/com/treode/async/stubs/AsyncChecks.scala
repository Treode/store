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

package com.treode.async.stubs

import java.util.concurrent.Executors
import scala.util.Random

import com.treode.async.Scheduler
import org.scalatest.{Informing, Suite}

trait AsyncChecks {
  this: Suite with Informing =>

  /** The number of times to run a psuedo-random test. Uses `NSEEDS` from the environment, or
    * defaults to 1.
    */
  val nseeds: Int = {
    val envseeds = System.getenv("NSEEDS")
    if (envseeds == null) 1 else Integer.parseInt(envseeds)
  }

  /** Run the test with `seed`; add `seed` to the test info on failure. */
  def forSeed (seed: Long) (test: Long => Any) {
    try {
      test (seed)
    } catch {
      case t: Throwable =>
        info (s"Test failed; seed = ${seed}L")
        throw t
    }}

  /** Run a test many times, each time with a different seed. Use `NSEEDS` from the environment to
    * determine how many times, or defaults to 1.
    */
  def forAllSeeds (test: Long => Any) {
    for (_ <- 0 until nseeds)
      forSeed (Random.nextLong) (test)
  }

  /** Run a psuedo-random test many times, each time with a PRNG seeded differently. Use `NSEEDS`
    * from the environment to determine how many times, or defaults to 1.
    */
  def forAllRandoms (test: Random => Any): Unit =
    forAllSeeds (seed => test (new Random (seed)))
}
