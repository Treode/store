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

import com.treode.disk.Position
import com.treode.store.{Atlas, Cohort, Residents, StoreTestConfig}
import org.scalatest.FreeSpec

import Cohort.settled
import Residents.all
import TierTestTools._

class TiersSpec extends FreeSpec {

  val config = StoreTestConfig()
  import config._

  private def residents (cohorts: Int*): Residents =
    Atlas (cohorts .map (settled (_)) .toArray, 1) .residents (0)

  private def tier (gen: Int, res: Residents = all, keys: Int = 1, bytes: Int = 0): Tier = {
    val pos = Position (-1, -1, -1)
    Tier (gen, pos, pos, res, keys, 0, 0, 0, bytes)
  }

  private def tiers (ts: Tier*): Tiers =
    new Tiers (ts)

  "When Tiers.choose is given" - {

    "no tiers, it should" - {

      val ts = tiers()

      "not barf" in {
        assertResult (tiers()) {
          ts.choose (Set.empty, Residents.all)
        }}}

    "one tier and" - {

      val t1 = tier (2, residents (0))
      val ts = tiers (t1)

      "no gens or migration, it should choose nothing" in {
        assertResult (tiers()) {
          ts.choose (Set.empty, all)
        }}

      "a missing gen and no migration, it should choose nothing" in {
        assertResult (tiers()) {
          ts.choose (Set (1), all)
        }}

      "the gen of the tier, it should choose the tier" in {
        assertResult (tiers (t1)) {
          ts.choose (Set (2), all)
        }}

      "an exodus, it should choose the tier" in {
        assertResult (tiers (t1)) {
          ts.choose (Set (1), residents (0, 1))
        }}}

    "two tiers and" - {

      val t1 = tier (2, residents (0, 0, 1, 1))
      val t2 = tier (3, residents (0, 1, 0, 1))
      val ts = tiers (t2, t1)

      "no gens or migration, it should choose nothing" in {
        assertResult (tiers()) {
          ts.choose (Set.empty, all)
        }}

      "a missing gen and no migration, it should choose nothing" in {
        assertResult (tiers()) {
          ts.choose (Set (1), all)
        }}

      "the gen of the older tier, it should choose both tiers" in {
        assertResult (tiers (t2, t1)) {
          ts.choose (Set (2), all)
        }}

      "the gen of the younger tier, it should choose just that tier" in {
        assertResult (tiers (t2)) {
          ts.choose (Set (3), all)
        }}

      "an exodus from the older tier, it should choose both tiers" in {
        assertResult (tiers (t2, t1)) {
          ts.choose (Set.empty, residents (0, 1, 0, 1))
        }}

      "an exodus from the younger tier, it should choose just that tier" in {
        assertResult (tiers (t2)) {
          ts.choose (Set.empty, residents (0, 0, 1, 1))
        }}

      "the younger is larger than the older, it should choose both tiers" in {
        val t1 = tier (2, bytes = 64)
        val t2 = tier (3, bytes = 128)
        val ts = tiers (t2, t1)
        assertResult (tiers (t2, t1)) {
          ts.choose (Set.empty, Residents.all)
        }}}}

  "When Tiers.compact is given" - {

    val t1 = tier (1)
    val t1e = tier (1, keys = 0)
    val t2 = tier (2)
    val t2e = tier (2, keys = 0)
    val t3 = tier (3)
    val t3e = tier (3, keys = 0)

    "no tiers, it should yield the one new tier" in {
      assertResult (tiers (t1)) {
        tiers() .compact (Long.MaxValue, t1)
      }}

    "no tiers and an empty new tier, it should yield no tiers" in {
      assertResult (tiers()) {
        tiers() .compact (Long.MaxValue, t1e)
      }}

    "one tier, and a new tier replacing nothing, it should work" in {
      assertResult (tiers (t2, t1)) {
        tiers (t1) .compact (Long.MaxValue, t2)
      }}

    "one tier, and a new empty tier replacing nothing, it should work" in {
      assertResult (tiers (t1)) {
        tiers (t1) .compact (Long.MaxValue, t2e)
      }}

    "one tier, and a new tier replacing it, it should work" in {
      assertResult (tiers (t2)) {
        tiers (t1) .compact (t1.gen, t2)
      }}

    "one tier, and a new empty tier replacing it, it should work" in {
      assertResult (tiers()) {
        tiers (t1) .compact (t1.gen, t2e)
      }}

    "two tiers, and a new one replacing the younger, it should work" in {
      assertResult (tiers (t1)) {
        tiers (t2, t1) .compact (t2.gen, t3e)
      }}

    "two tiers, and a new empty one replacing the younger, it should work" in {
      assertResult (tiers (t1)) {
        tiers (t2, t1) .compact (t2.gen, t3e)
      }}

    "two tiers, and a new one replacing the older, it should work" in {
      assertResult (tiers (t3, t2)) {
        tiers (t3, t1) .compact (t1.gen, t2)
      }}

    "two tiers, and a new empty one replacing the older, it should work" in {
      assertResult (tiers (t3)) {
        tiers (t3, t1) .compact (t1.gen, t2e)
      }}

    "two tiers, and a new one replacing both, it should work" in {
      assertResult (tiers (t3)) {
        tiers (t2, t1) .compact (t1.gen, t3)
      }}

     "two tiers, and a new empty one replacing both, it should work" in {
      assertResult (tiers()) {
        tiers (t2, t1) .compact (t1.gen, t3e)
      }}}}
