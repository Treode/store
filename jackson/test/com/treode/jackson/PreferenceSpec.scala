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

import java.net.InetSocketAddress

import com.treode.store.Preference
import org.scalatest.FlatSpec

class PreferenceSpec extends FlatSpec with ModuleSpec {

  "Serializing a preference" should "work" in {
    assertString ("""{"weight":1,"hostId":"0x8BB6E8637E8E0356","addr":"localhost:80","sslAddr":null}""") {
      Preference (1, 0x8BB6E8637E8E0356L, Some (new InetSocketAddress ("localhost", 80)), None)
    }}
}
