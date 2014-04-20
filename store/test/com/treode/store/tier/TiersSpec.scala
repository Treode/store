package com.treode.store.tier

import com.treode.disk.Position
import com.treode.store.{Cohort, Residents}
import org.scalatest.FreeSpec

import Cohort.settled
import Residents.all
import TierTestTools._

class TiersSpec extends FreeSpec {

  implicit val config = TestStoreConfig()

  private def residents (cohorts: Int*): Residents =
    Residents (0, cohorts .map (settled (_)) .toArray)

  private def tier (gen: Int, res: Residents = all, bytes: Int = 0): Tier = {
    val pos = Position (-1, -1, -1)
    Tier (gen, pos, pos, res, 0, 0, 0, 0, bytes)
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

  "When Tiers.compacted is given" - {

    val t1 = tier (1)
    val t2 = tier (2)
    val t3 = tier (3)

    "no tiers, it should yield the one new tier" in {
      assertResult (tiers (t1)) {
        tiers() .compacted (t1, tiers())
      }}

    "one tier, the new tier replacing nothing, it should work" in {
      assertResult (tiers (t2, t1)) {
        tiers (t1) .compacted (t2, tiers())
      }}

    "one tier, the new tier replacing it, it should work" in {
      assertResult (tiers (t2)) {
        tiers (t1) .compacted (t2, tiers (t1))
      }}

    "two tiers, the new one replacing the younger, it should work" in {
      assertResult (tiers (t3, t1)) {
        tiers (t2, t1) .compacted (t3, tiers (t2))
      }}

    "two tiers, the new one replacing the older, it should work" in {
      assertResult (tiers (t3, t2)) {
        tiers (t3, t1) .compacted (t2, tiers (t1))
      }}

    "two tiers, the new one replacing both, it should work" in {
      assertResult (tiers (t3)) {
        tiers (t2, t1) .compacted (t3, tiers (t2, t1))
      }}}}
