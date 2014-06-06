package com.treode.store

import org.scalatest.FreeSpec
import com.treode.cluster.HostId

import Cohort.settled

class AtlasSpec extends FreeSpec {

  val H1 = HostId (0x44L)
  val H2 = HostId (0x75L)
  val H3 = HostId (0xC3L)
  val H4 = HostId (0x7EL)
  val H5 = HostId (0x8CL)
  val H6 = HostId (0x32L)
  val H7 = HostId (0xEDL)
  val H8 = HostId (0xBFL)

  val cohorts = Array (
      settled (H1, H2, H6),
      settled (H1, H3, H7),
      settled (H1, H4, H8),
      settled (H2, H3, H8),
      settled (H2, H4, H7),
      settled (H3, H5, H6),
      settled (H4, H5, H8),
      settled (H5, H6, H7))

  val atlas = Atlas (cohorts, 1)

  "Atlas.hosts when" - {

    "the number of slices is less than the cohort map" in {
      assertResult (Map (H1 -> 1, H2 -> 1, H3 -> 3, H5 -> 2, H6 -> 2, H7 -> 2,  H8 -> 1)) {
        atlas.hosts (Slice (1, 2)) .toMap
      }
      assertResult (Map (H1 -> 1, H3 -> 2, H5 -> 1, H6 -> 1, H7 -> 1)) {
        atlas.hosts (Slice (1, 4)) .toMap
      }}

    "the number of slices equals the cohort map" in {
      assertResult (Map (H1 -> 1, H3 -> 1, H7 -> 1)) {
        atlas.hosts (Slice (1, 8)) .toMap
      }}

    "the number of slices is larger than the cohort map" in {
      assertResult (Map (H1 -> 1, H3 -> 1, H7 -> 1)) {
        atlas.hosts (Slice (1, 16)) .toMap
      }}}}
