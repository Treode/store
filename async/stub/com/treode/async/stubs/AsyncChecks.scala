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

  /** The value of `TEST_INTENSITY` from the environment, or `std` if the environment has no
    * setting for that.
    */
  val intensity: String = {
    val env = System.getenv ("TEST_INTENSITY")
    if (env == null) "std" else env
  }

  /** The number of seeds tried in `forAllSeeds`.  1 when when `intensity` is `dev` and 100
    * otherwise.
    */
  val nseeds =
    intensity match {
      case "dev" => 1
      case _ => 100
    }

  /** Run the test with a PRNG seeded by `seed`. */
  def forSeed (seed: Long) (test: Random => Any) {
    try {
      val random = new Random (seed)
      test (random)
    } catch {
      case t: Throwable =>
        info (s"Test failed; seed = ${seed}L")
        throw t
    }}

  /** Run the test many times, each time with a PRNG seeded differently.  When developing, set
    * the environment variable `TEST_INTENSITY` to `dev` to run the test only once.  Let your
    * continuous build spend the time running it over and over.
    */
  def forAllSeeds (test: Random => Any) {
    for (_ <- 0 until nseeds)
      forSeed (Random.nextLong) (test)
  }}
