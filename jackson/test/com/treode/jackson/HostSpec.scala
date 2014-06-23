package com.treode.jackson

import com.treode.cluster.HostId
import org.scalatest.FreeSpec

class HostSpec extends FreeSpec with ModuleSpec {

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

    "handle a large hexadecimal string" in {
      accept (HostId (0xFCFE52C72C64CABAL)) ("\"0xFCFE52C72C64CABA\"")
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
    }}}
