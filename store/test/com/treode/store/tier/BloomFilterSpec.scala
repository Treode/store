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

package com.treode.store.tier

import scala.util.Random

import com.treode.store.{Bytes, Fruits}
import com.treode.pickle.Picklers
import org.scalatest.FreeSpec

import BloomFilter.{optimalNumBits, optimalNumHashes}
import Fruits.{AllFruits, Apple}

class BloomFilterSpec extends FreeSpec {

  "BloomFilter.optimalNumBits should" - {

    def check (numKeys: Int, fpp: Double, min: Int, max: Int) {
      val numBits = optimalNumBits (numKeys, fpp)
      assert (
          min * numKeys <= numBits && numBits <= max * numKeys,
          s"Expected $numBits between ${min * numKeys} and ${max * numKeys}")
    }

    "compute 6 to 7 bits per key for a false positive probabity of 5%" in {
      check (100, 0.05, 6, 7)
      check (1000, 0.05, 6, 7)
      check (10000, 0.05, 6, 7)
    }

    "compute 7 to 8 bits per key for a false positive probabity of 3%" in {
      check (100, 0.03, 7, 8)
      check (1000, 0.03, 7, 8)
      check (10000, 0.03, 7, 8)
    }

    "compute 9 to 10 bits per key for a false positive probabity of 1%" in {
      check (100, 0.01, 9, 10)
      check (1000, 0.01, 9, 10)
      check (10000, 0.01, 9, 10)
    }}

  "BloomFilter.optimalNumHashes should" - {

    "use 4 hashes for 6 bits per key" in {
      assert (optimalNumHashes (1000, 6000) == 4)
    }

    "use 6 hashes for 8 bits per key" in {
      assert (optimalNumHashes (1000, 8000) == 6)
    }

    "use 7 hashes for 10 bits per key" in {
      assert (optimalNumHashes (1000, 10000) == 7)
    }}

  "The BloomFilter should" - {

    def check (fpp: Double, limit: Int) {
      val bloom = BloomFilter (AllFruits.length, fpp)
      for (f <- AllFruits)
        bloom.put (Bytes.pickler, f)
      for (f <- AllFruits)
        assert (bloom.contains (Bytes.pickler, f))
      var fp = 0
      for (_ <- 0 until 1000)
        if (bloom.contains (Picklers.int, Random.nextInt))
          fp += 1
      assert (fp <= limit * 10)
    }

    "yield false positives near the desired probability, that is" - {

      // It's just a probability, not a certainty.  We need to allow some room so that the tests
      // don't become flakey.

      "7/100 or less for 5%" in {
        check (0.05, 7)
      }

      "5/100 or less for 3%" in {
        check (0.03, 5)
      }

      "2/100 or less for 1%" in {
        check (0.01, 2)
      }}}

  "handle whacky constructor parameters" in {
    val bloom = BloomFilter (1, 0.99)
    bloom.put (Bytes.pickler, Apple)
    assert (bloom.contains (Bytes.pickler, Apple))
  }}
