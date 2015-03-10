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

package com.treode.async.misc

import org.scalatest.WordSpec

class EpochReleaserSpec extends WordSpec {

  private class TestReleaser () {

    val releaser = new EpochReleaser
    var released = Seq.empty [Int]

    def join(): Int =
      releaser. join()

    def leaveAndExpect (epoch: Int) (fs: Int*) {
      released = Seq.empty
      releaser.leave (epoch)
      assertResult (fs) (released)
    }

    def releaseAndExpect (ns: Int*) (fs: Int*) {
      released = Seq.empty
      releaser.release (released ++= ns)
      assertResult (fs) (released)
    }}

  "The EpochReleaser" when {

    // release
    "there are no previous epochs and no parties" should {
      "release immediately" in {
        val releaser = new TestReleaser
        releaser.releaseAndExpect (0) (0)
      }}

    // join, release, leave
    "there are actions and a party" should {
      "release when the party leaves" in {
        val releaser = new TestReleaser
        val e1 = releaser.join()
        releaser.releaseAndExpect (0) ()
        releaser.leaveAndExpect (e1) (0)
      }}

    "there is a previous epoch and a party" should {

      // join(1), release, join(2), release, leave(1), leave(2)
      "free when the previous epoch frees and then the party leaves" in {
        val releaser = new TestReleaser
        val e1 = releaser.join()
        releaser.releaseAndExpect (0) ()
        val e2 = releaser.join()
        releaser.releaseAndExpect (1) ()
        releaser.leaveAndExpect (e1) (0)
        releaser.leaveAndExpect (e2) (1)
      }

      // join(1), release, join(2), release, leave(2), leave(1)
      "free when the party leaves and then the previous epoch frees" in {
        val releaser = new TestReleaser
        val e1 = releaser.join()
        releaser.releaseAndExpect (0) ()
        val e2 = releaser.join()
        releaser.releaseAndExpect (1) ()
        releaser.leaveAndExpect (e2) ()
        releaser.leaveAndExpect (e1) (0, 1)
      }}}}
