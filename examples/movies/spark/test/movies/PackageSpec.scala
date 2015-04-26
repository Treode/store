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

package movies

import java.net.URI

import org.scalatest.FlatSpec

class PackageSpec extends FlatSpec {

  "urlOfPartition" should "work" in {
    assertResult ("http://example.com?slice=3&nslices=8") {
      urlOfPartition (new URI ("http://example.com"), 3, 8) .toString
    }}

  "urlOfWidnow" should "work" in {
    assertResult ("http://example.com?slice=3&nslices=8&since=100&until=200") {
      urlOfWindow (new URI ("http://example.com"), 3, 8, 100, 200) .toString
    }}}
