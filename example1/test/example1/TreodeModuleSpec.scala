package example1

import java.nio.file.{Path, Paths}

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.treode.disk.{DriveAttachment, DriveDigest, DriveGeometry}
import com.treode.cluster.HostId
import com.treode.store.Cohort
import org.scalatest.FreeSpec

import Cohort._

class TreodeModuleSpec extends FreeSpec {

  val mapper = new ObjectMapper()
  mapper.registerModule (DefaultTreodeModule)

  def assertString (expected: String) (input: Any): Unit =
    assertResult (expected) (mapper.writeValueAsString (input))

  def accept [A] (expected: A) (input: String) (implicit m: Manifest [A]): Unit =
    assertResult (expected) (mapper.readValue (input, m.runtimeClass))

  def reject [A] (input: String) (implicit m: Manifest [A]): Unit =
    intercept [JsonProcessingException] (mapper.readValue (input, m.runtimeClass))

  "Serializing a path should" - {

    "produce a string" in {
      assertString ("\"/a\"") (Paths.get ("/a"))
    }}

  "Deserializing a path should" - {

    "read a sring" in {
      accept (Paths.get ("/a")) ("\"/a\"")
    }

    "reject an integer" in {
      reject [Path] ("1")
    }

    "reject a float" in {
      reject [Path] ("1.0")
    }

    "reject an array" in {
      reject [Path] ("[]")
    }

    "reject an object" in {
      reject [Path] ("{}")
    }}

  "Serializing a drive attachment should" - {

    "work" in {
      assertString ("""{"path":"/a","geometry":{"segmentBits":30,"blockBits":13,"diskBytes":1099511627776}}""") {
        DriveAttachment (Paths.get ("/a"), DriveGeometry (30, 13, 1L<<40))
      }}}

  "Deserializing a drive attachment should" - {

    "work" in {
      accept (DriveAttachment (Paths.get ("/a"), DriveGeometry (30, 13, 1L<<40))) {
        """{"path":"/a","geometry":{"segmentBits":30,"blockBits":13,"diskBytes":1099511627776}}"""
      }}

    "reject an attachment with a bad path" in {
      reject [DriveAttachment] {
        """{"path": 1, geometry: {"segmentBits":30,"blockBits":13,"diskBytes":1099511627776}}"""
      }}

    "reject an attachment with bad geometry values" in {
      reject [DriveAttachment] {
        """{"path": "/a", geometry: {"segmentBits":-1,"blockBits":-1,"diskBytes":-1}}"""
      }}

    "reject an attachment with a bad geometry object" in {
      reject [DriveAttachment] {
        """{"path": "/a", geometry: 1}"""
      }}

    "reject an empty object" in {
      reject [DriveGeometry] ("{}")
    }

    "reject an integer" in {
      reject [DriveGeometry] ("1")
    }

    "reject a float" in {
      reject [DriveGeometry] ("1.0")
    }

    "reject an array" in {
      reject [DriveGeometry] ("[]")
    }}

  "Serializing a drive digest should" - {

    "work" in {
      assertString ("""{"path":"/a","geometry":{"segmentBits":30,"blockBits":13,"diskBytes":1099511627776},"allocated":1,"draining":false}""") {
        DriveDigest (Paths.get ("/a"), DriveGeometry (30, 13, 1L<<40), 1, false)
      }}}

  "Serializing drive geometry should" - {

    "work" in {
      assertString ("""{"segmentBits":30,"blockBits":13,"diskBytes":1099511627776}""") {
        DriveGeometry (30, 13, 1L<<40)
      }}}

  "Deserializing drive geometry should" - {

    "work" in {
      accept (DriveGeometry (30, 13, 1L<<40)) {
        """{"segmentBits":30,"blockBits":13,"diskBytes":1099511627776}"""
      }}

    "reject a geometry with bad values" in {
      reject [DriveGeometry] {
        """{"segmentBits":-1,"blockBits":-1,"diskBytes":-1}"""
      }}

    "reject an empty object" in {
      reject [DriveGeometry] ("{}")
    }

    "reject an integer" in {
      reject [DriveGeometry] ("1")
    }

    "reject a float" in {
      reject [DriveGeometry] ("1.0")
    }

    "reject an array" in {
      reject [DriveGeometry] ("[]")
    }}

  "Serializing a HostId should" - {

    "produce a hex string with leading zeros" in {
      assertString ("\"0x00574007A4386A1A\"") (HostId (0x00574007A4386A1AL))
    }}

  "Deserializing a HostId should" - {

    "handle an decimal long" in {
      accept (HostId (0x778EE7AD8196BB93L)) ("8615077869595835283")
    }

    "handle a hexadecimal string" in {
      accept (HostId (0x778EE7AD8196BB93L)) ("\"0x778EE7AD8196BB93\"")
    }

    "handle a host string" in {
      accept (HostId (0x778EE7AD8196BB93L)) ("\"Host:778EE7AD8196BB93\"")
    }

    "reject a bad string" in {
      reject [HostId] ("\"hst:AB\"")
    }

    "reject a float" in {
      reject [HostId] ("1.0")
    }

    "reject an array" in {
      reject [HostId] ("[]")
    }

    "reject an object" in {
      reject [HostId] ("{}")
    }}

  "Deserializing a cohort should" - {

    "reject an empty object" in {
      reject [Cohort] ("{}")
    }

    "reject an integer" in {
      reject [Cohort] ("1")
    }

    "reject a float" in {
      reject [Cohort] ("1.0")
    }

    "reject a string" in {
      reject [Cohort] ("\"hello\"")
    }

    "reject an array" in {
      reject [Cohort] ("[]")
    }}

  "Serializing an empty cohort should" - {

    "produce an object with state:empty" in {
      assertString ("""{"state":"empty"}""") (Empty)
    }}

  "Deserializing an empty cohort should" - {

    "work" in {
      accept [Cohort] (Empty) {
        """{"state":"empty"}"""
      }}

    "reject an object with hosts" in {
      reject [Cohort] {
        """{"state":"empty", "hosts":[1]}"""
      }}

    "reject an object with an origin" in {
      reject [Cohort] {
        """{"state":"empty", "origin":[1]}"""
      }}

    "reject an object with a target" in {
      reject [Cohort] {
        """{"state":"empty", "target":[1]}"""
      }}}

  "Serializing a settled cohort should" - {

    "produce an object with state:settled and hosts" in {
      assertString ("""{"state":"settled","hosts":["0x0000000000000001"]}""") {
        settled (1)
      }}}

  "Deserializing a settled cohort should" - {

    "work" in {
      accept (settled (1)) {
        """{"state":"settled", "hosts":[1]}"""
      }}

    "reject an object with no hosts" in {
      reject [Cohort] {
        """{"state":"settled", "hosts":[]}"""
      }}

    "reject an object with an origin" in {
      reject [Cohort] {
        """{"state":"settled", "origin":[1]}"""
      }}

    "reject an object with a target" in {
      reject [Cohort] {
        """{"state":"settled", "target":[1]}"""
      }}}

  "Serializing an issuing cohort should" - {

    "produce an object with state:issuing, origin and target" in {
      assertString ("""{"state":"issuing","origin":["0x0000000000000001"],"target":["0x0000000000000002"]}""") {
        issuing (1) (2)
      }}}

  "Deserializing an issuing cohort should" - {

    "work" in {
      accept (issuing (1) (2)) {
        """{"state":"issuing", "origin":[1], "target":[2]}"""
      }}

    "reject an object with hosts" in {
      reject [Cohort] {
        """{"state":"issuing", "hosts":[1], "origin":[1], "target":[2]"}"""
      }}

    "reject an object with no origin" in {
      reject [Cohort] {
        """{"state":"issuing", "origin":[], "target":[2]"}"""
      }}

    "reject an object with no target" in {
      reject [Cohort] {
        """{"state":"issuing", "origin":[1], "target":[]"}"""
      }}}

  "Serializing a moving cohort should" - {

    "produce an object with state:moving, origin and target" in {
      assertString ("""{"state":"moving","origin":["0x0000000000000001"],"target":["0x0000000000000002"]}""") {
        moving (1) (2)
      }}}

  "Deserializing a moving cohort should" - {

    "work" in {
      accept (moving (1) (2)) {
        """{"state":"moving", "origin":[1], "target":[2]}"""
      }}

    "reject an object with hosts" in {
      reject [Cohort] {
        """{"state":"moving", "hosts":[1], "origin":[1], "target":[2]"}"""
      }}

    "reject an object with no origin" in {
      reject [Cohort] {
        """{"state":"moving", "origin":[], "target":[2]"}"""
      }}

    "reject an object with no target" in {
      reject [Cohort] {
        """{"state":"moving", "origin":[1], "target":[]"}"""
      }}}}
