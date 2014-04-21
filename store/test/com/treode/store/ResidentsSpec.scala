package com.treode.store

import scala.util.Random
import org.scalatest.FlatSpec

import Cohort.settled
import StoreTestTools._

class ResidentsSpec extends FlatSpec {

  private def residents (cohorts: Int*): Residents =
    Cohorts (cohorts .map (settled (_)) .toArray, 1) .residents (0)

  "Residents.all" should "contain all ids" in {
    for (id <- Stream.fill (100) (Random.nextInt))
      assert (Residents.all.contains (id))
  }

  "Residents.contains" should "contain only local cohorts" in {
    val rs = residents (0, 1, 0, 2)
    assert (rs.contains (0))
    assert (rs.contains (2))
    assert (!rs.contains (1))
    assert (!rs.contains (3))
  }

  "Residents.stability" should "return the percentage of still-hosted cohorts" in {

    assertResult (1.0D) ((Residents.all) stability (residents (0)))
    assertResult (0.5D) ((Residents.all) stability (residents (0, 1)))
    assertResult (0.25D) ((Residents.all) stability (residents (0, 1, 2, 3)))

    assertResult (1.0D) ((residents (0)) stability (Residents.all))
    assertResult (1.0D) ((residents (0, 1)) stability (Residents.all))
    assertResult (1.0D) ((residents (0, 1, 2, 3)) stability (Residents.all))

    assertResult (1.0D) ((residents (0, 1)) stability (residents (0, 1)))
    assertResult (0.0D) ((residents (0, 1)) stability (residents (2, 1)))
    assertResult (1.0D) ((residents (0, 1)) stability (residents (0, 1, 0, 2)))
    assertResult (0.5D) ((residents (0, 1)) stability (residents (0, 1, 2, 3)))
    assertResult (1.0D) ((residents (1, 2)) stability (residents (0, 1)))
    assertResult (1.0D) ((residents (0, 1, 0, 2)) stability (residents (0, 1)))
    assertResult (1.0D) ((residents (0, 1, 2, 3)) stability (residents (0, 1)))

    assertResult (0.5D) ((residents (0, 1, 0, 3)) stability (residents (0, 1, 2, 3)))
    assertResult (0.25D) {
      (residents (0, 1, 0, 3, 0, 5, 0, 7)) stability (residents (0, 1, 2, 3, 4, 5, 6, 7))
    }}}
