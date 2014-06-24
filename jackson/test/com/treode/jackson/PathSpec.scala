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
