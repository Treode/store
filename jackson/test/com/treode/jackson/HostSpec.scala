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
