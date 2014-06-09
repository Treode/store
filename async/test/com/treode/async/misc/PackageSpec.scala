package com.treode.async.misc

import org.scalatest.FlatSpec

class PackageSpec extends FlatSpec {

  "parseUnsignedLong" should "work" in {
    assertResult (Some (0xFECDB005DA3B9A60L)) (parseUnsignedLong ("0xFECDB005DA3B9A60"))
    assertResult (Some (0xFECDB005DA3B9A60L)) (parseUnsignedLong ("#FECDB005DA3B9A60"))
    assertResult (Some (64)) (parseUnsignedLong ("0100"))
    assertResult (Some (10L)) (parseUnsignedLong ("10"))
    assertResult (Some (-1)) (parseUnsignedLong ("0xFFFFFFFFFFFFFFFF"))
    assertResult (None) (parseUnsignedLong ("0x1FFFFFFFFFFFFFFFF"))
  }}
