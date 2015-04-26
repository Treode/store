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

package com.treode.twitter

import com.treode.async.misc.RichOption
import com.treode.cluster.{CellId, HostId}
import com.twitter.app.Flaggable

package object app {

  /** Parse a flag as a CellId. Handles decimal (no leading zero), octal (leading zero) or
    * hexadecimal (leading `0x` or `#`). Doesn't mind large values that flip the sign. Accepts
    * positive values too large for 63 bits, but still rejects values too large for 64 bits.
    */
  implicit val flaggableCellId: Flaggable [CellId] =
    Flaggable.mandatory { s =>
      CellId.parse (s) .getOrThrow (new IllegalArgumentException (s"not a cell-id $s"))
    }

  /** Parse a flag as a HostId. Handles decimal (no leading zero), octal (leading zero) or
    * hexadecimal (leading `0x` or `#`). Doesn't mind large values that flip the sign. Accepts
    * positive values too large for 63 bits, but still rejects values too large for 64 bits.
    */
  implicit val flaggableHostId: Flaggable [HostId] =
    Flaggable.mandatory { s =>
      HostId.parse (s) .getOrThrow (new IllegalArgumentException (s"not a host-id $s"))
    }}
