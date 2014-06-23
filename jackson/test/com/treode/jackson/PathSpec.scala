package com.treode.jackson

import java.nio.file.{Path, Paths}
import org.scalatest.FreeSpec

class PathSpec extends FreeSpec with ModuleSpec {

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
    }}}
