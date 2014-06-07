package com.treode.store

import org.scalatest.FreeSpec
import com.treode.cluster.HostId

import Cohort.{issuing, moving, settled}

class AtlasSpec extends FreeSpec {

  val H1 = HostId (0x44L)
  val H2 = HostId (0x75L)
  val H3 = HostId (0xC3L)
  val H4 = HostId (0x7EL)
  val H5 = HostId (0x8CL)
  val H6 = HostId (0x32L)
  val H7 = HostId (0xEDL)
  val H8 = HostId (0xBFL)

  val HS = Seq (H1, H2, H3, H4, H5, H6, H7, H8)

  val hostsAt1 = Map.empty [HostId, Int] .set (1) (HS: _*)
  val hostsAt2 = Map.empty [HostId, Int] .set (2) (HS: _*)

  implicit class RichHostMap (map: Map [HostId, Int]) {

    def set (version: Int) (hosts: HostId*): Map [HostId, Int] = {
      var m = map
      for (h <- hosts)
        m += h -> version
      m
    }}

  def atlas (version: Int) (cohorts: Cohort*): Atlas =
    Atlas (cohorts.toArray, version)

  "Atlas.hosts should yield the prefered hosts when" - {

    val a = atlas (1) (
        settled (H1, H2, H6),
        settled (H1, H3, H7),
        settled (H1, H4, H8),
        settled (H2, H3, H8),
        settled (H2, H4, H7),
        settled (H3, H5, H6),
        settled (H4, H5, H8),
        settled (H5, H6, H7))

    "the number of slices is less than the cohort map" in {
      assertResult (Map (H1 -> 1, H2 -> 1, H3 -> 3, H5 -> 2, H6 -> 2, H7 -> 2,  H8 -> 1)) {
        a.hosts (Slice (1, 2)) .toMap
      }
      assertResult (Map (H1 -> 1, H3 -> 2, H5 -> 1, H6 -> 1, H7 -> 1)) {
        a.hosts (Slice (1, 4)) .toMap
      }}

    "the number of slices equals the cohort map" in {
      assertResult (Map (H1 -> 1, H3 -> 1, H7 -> 1)) {
        a.hosts (Slice (1, 8)) .toMap
      }}

    "the number of slices is larger than the cohort map" in {
      assertResult (Map (H1 -> 1, H3 -> 1, H7 -> 1)) {
        a.hosts (Slice (1, 16)) .toMap
      }}}

  "Atlas.advance should" - {

    "not change an issuing cohorts when the receipts are behind" in {
      assertResult (None) {
        atlas (2) (issuing (H1, H2, H3) (H4, H5, H6))
            .advance (hostsAt1, hostsAt1)
      }}

    "change issuing to moving when a quorum is current" in {
      assertResult {
        atlas (3) (moving (H1, H2, H3) (H4, H5, H6))
      } {
        atlas (2) (issuing (H1, H2, H3) (H4, H5, H6))
            .advance (hostsAt1.set (2) (H1, H2, H4, H5), hostsAt1)
            .get
      }}

    "not change a moving cohort when the moves are behind" in {
      assertResult (None) {
        atlas (2) (moving (H1, H2, H3) (H4, H5, H6))
            .advance (hostsAt2, hostsAt1)
      }}

    "changing moving to settled when a quorum have moved" in {
      assertResult{
        atlas (3) (settled (H4, H5, H6))
      } {
        atlas (2) (moving (H1, H2, H3) (H4, H5, H6))
            .advance (hostsAt2, hostsAt1.set (2) (H1, H2, H4, H5))
            .get
      }}}}
