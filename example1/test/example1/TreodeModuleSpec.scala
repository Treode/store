package example1

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.treode.cluster.HostId
import com.treode.store.Cohort
import org.scalatest.FreeSpec

import Cohort._

class TreodeModuleSpec extends FreeSpec {

  val mapper = new ObjectMapper()
  mapper.registerModule (TreodeModule)

  def assertString (expected: String) (input: Any): Unit =
    assertResult (expected) (mapper.writeValueAsString (input))

  def assertCohort (expected: Cohort) (input: String): Unit =
    assertResult (expected) (mapper.readValue (input, classOf [Cohort]))

  def rejectCohort (input: String): Unit =
    intercept [JsonProcessingException] (mapper.readValue (input, classOf [Cohort]))

  "Serializing a HostId should" - {

    "produce a hex string with leading zeros" in {
      assertString ("\"0x00574007A4386A1A\"") (HostId (0x00574007A4386A1AL))
    }}

  "Deserializing a HostId should" - {

    "handle an decimal long" in {
      assertResult (HostId (0x778EE7AD8196BB93L)) {
        mapper.readValue ("8615077869595835283", classOf [HostId])
      }}

    "handle a hexadecimal string" in {
      assertResult (HostId (0x778EE7AD8196BB93L)) {
        mapper.readValue ("\"0x778EE7AD8196BB93\"", classOf [HostId])
      }}

    "handle a host string" in {
      assertResult (HostId (0x778EE7AD8196BB93L)) {
        mapper.readValue ("\"Host:778EE7AD8196BB93\"", classOf [HostId])
      }}

    "reject a bad string" in {
      intercept [JsonProcessingException] {
        mapper.readValue ("\"hst:AB\"", classOf [HostId])
      }}

    "reject a float" in {
      intercept [JsonProcessingException] {
        mapper.readValue ("1.0", classOf [HostId])
      }}

    "reject an array" in {
      intercept [JsonProcessingException] {
        mapper.readValue ("[]", classOf [HostId])
      }}

    "reject an object" in {
      intercept [JsonProcessingException] {
        mapper.readValue ("{}", classOf [HostId])
      }}}

  "Deserializing a cohort should" - {

    "reject an object with no state or hosts" in {
      rejectCohort {
        """{}"""
      }}

    "reject an integer" in {
      rejectCohort {
        "1"
      }}

    "reject a float" in {
      rejectCohort {
        "1.0"
      }}

    "reject a string" in {
      rejectCohort {
        "\"hello\""
      }}

    "reject an array" in {
      rejectCohort {
        """[]"""
      }}}

  "Serializing a settled cohort should" - {

    "produce an object with state:settled and hosts" in {
      assertString ("""{"state":"settled","hosts":["0x0000000000000001"]}""") {
        settled (1)
      }}}

  "Deserializing a settled cohort should" - {

    "work" in {
      assertCohort (settled (1)) {
        """{"state":"settled", "hosts":[1]}"""
      }}

    "reject an object with no hosts" in {
      rejectCohort {
        """{"state":"settled", "hosts":[]}"""
      }}

    "reject an object with an origin" in {
      rejectCohort {
        """{"state":"settled", "origin":[1]}"""
      }}

    "reject an object with a target" in {
      rejectCohort {
        """{"state":"settled", "target":[1]}"""
      }}}

  "Serializing an issuing cohort should" - {

    "produce an object with state:issuing, origin and target" in {
      assertString ("""{"state":"issuing","origin":["0x0000000000000001"],"target":["0x0000000000000002"]}""") {
        issuing (1) (2)
      }}}

  "Deserializing an issuing cohort should" - {

    "work" in {
      assertCohort (issuing (1) (2)) {
        """{"state":"issuing", "origin":[1], "target":[2]}"""
      }}

    "reject an object with hosts" in {
      rejectCohort {
        """{"state":"issuing", "hosts":[1], "origin":[1], "target":[2]"}"""
      }}

    "reject an object with no origin" in {
      rejectCohort {
        """{"state":"issuing", "origin":[], "target":[2]"}"""
      }}

    "reject an object with no target" in {
      rejectCohort {
        """{"state":"issuing", "origin":[1], "target":[]"}"""
      }}}

  "Serializing a moving cohort should" - {

    "produce an object with state:moving, origin and target" in {
      assertString ("""{"state":"moving","origin":["0x0000000000000001"],"target":["0x0000000000000002"]}""") {
        moving (1) (2)
      }}}

  "Deserializing a moving cohort should" - {

    "work" in {
      assertCohort (moving (1) (2)) {
        """{"state":"moving", "origin":[1], "target":[2]}"""
      }}

    "reject an object with hosts" in {
      rejectCohort {
        """{"state":"moving", "hosts":[1], "origin":[1], "target":[2]"}"""
      }}

    "reject an object with no origin" in {
      rejectCohort {
        """{"state":"moving", "origin":[], "target":[2]"}"""
      }}

    "reject an object with no target" in {
      rejectCohort {
        """{"state":"moving", "origin":[1], "target":[]"}"""
      }}}}
