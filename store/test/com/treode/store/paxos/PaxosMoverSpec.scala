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

package com.treode.store.paxos

import com.treode.async.stubs.StubScheduler
import com.treode.cluster.{Cluster, HostId, Peer}
import com.treode.cluster.stubs.StubPeer
import com.treode.store.{Atlas, Bytes, Cohort, StoreTestKit, StoreTestTools, TxClock}
import org.scalatest.{FreeSpec, ShouldMatchers}

import PaxosMover.{Point, Range, Targets, Tracker}
import Cohort.{issuing, moving, settled}
import StoreTestTools.{intToBytes, longToTxClock}

class PaxosMoverSpec extends FreeSpec with ShouldMatchers {

  private def targets (cohorts: Cohort*) (implicit cluster: Cluster): Targets =
    Targets (Atlas (cohorts.toArray, 1))

  private def begin (start: Int): Range =
    if (start == 0)
      Range (Point.Start, Point.End)
    else
      Range (Point.Middle (Bytes (start), TxClock.MaxValue), Point.End)

  private def range (start: Int, end: Int): Range =
    if (start == 0)
      Range (Point.Start, Point.Middle (Bytes (end), TxClock.MaxValue))
    else
      Range (
          Point.Middle (Bytes (start), TxClock.MaxValue),
          Point.Middle (Bytes (end), TxClock.MaxValue))

  def assertPeers (expected: HostId*) (actual: Set [Peer]): Unit =
    assertResult (expected.toSet) (actual map (_.id))

  def assertPeers (expected: Map [Int, Set [HostId]]) (actual: Map [Int, Set [Peer]]): Unit =
    assertResult (expected) (actual mapValues (_ map (_.id)))

  def assertTask (range: Range, targets: (Int, Set [HostId])*) (actual: Option [(Range, Targets)]) {
    assert (actual.isDefined)
    assertResult (range) (actual.get._1)
    assertPeers (targets.toMap) (actual.get._2.targets)
  }

  def assertNone (actual: Option [Any]): Unit =
    assertResult (None) (actual)

  private class RichTracker {

    val tracker = new Tracker

    def deque(): Option [(Range, Targets)] =
      tracker.deque()

    def continue (point: Point): Unit =
      tracker.continue (point)

    def continue (key: Int): Unit =
      tracker.continue (Point.Middle (Bytes (key), TxClock.MaxValue))

    def start (cohorts: Cohort*) (implicit cluster: Cluster): Unit =
      tracker.start (targets (cohorts: _*))
  }

  "Deriving targets from cohorts should" - {

    def setup() = {
      implicit val kit = StoreTestKit.random()
      import kit._
      implicit val cluster = new StubPeer (1)
      cluster
    }

    "find no targets when no cohorts are moving" in {
      implicit val cluster = setup()
      val ts = targets (
          settled (1, 2, 3),
          settled (4, 5, 6),
          settled (7, 8, 9),
          settled (10, 11, 12))
      assert (ts.isEmpty)
    }

    "find targets only from the cohorts that are moving" in {
      implicit val cluster = setup()
      val ts = targets (
          settled (1, 2, 3),
          issuing (1, 2, 3) (1, 2, 4),
          moving (1, 2, 3) (1, 2, 5),
          moving (1, 2, 3) (1, 6, 7))
      assert (!ts.isEmpty)
      assert (!(ts contains 0))
      assert (!(ts contains 1))
      assertPeers (2, 5) (ts (2))
      assertPeers (6, 7) (ts (3))
    }}

  "Points should" - {
    import Point.{End, Middle}

    "order properly" in {
      assert (Middle (0, 1) < Middle (0, 0))
      assert (Middle (0, 0) < Middle (1, 0))
      assert (Middle (0, 0) < End)
    }}

  "When the tracker" - {

    "no points of completed work and" - {

      "no work underway and" - {

        def setup() = {
          implicit val kit = StoreTestKit.random()
          import kit._
          implicit val cluster = new StubPeer (1)
          val tracker = new RichTracker
          (cluster, tracker)
        }

        "rebalance is not started, it should yield no work" in {
          implicit val (cluster, t) = setup()
          assertNone (t.deque())
        }

        "rebalance is started with all cohorts settled, it should yield no work" in {
          implicit val (cluster, t) = setup()
          t.start (settled (1, 2, 3))
          assertNone (t.deque())
          assertNone (t.deque())
        }

        "rebalance is started with a cohort moving, it should start work" in {
          implicit val (cluster, t) = setup()
          t.start (moving (1, 2, 3) (1, 2, 4))
          assertTask (begin (0), 0 -> Set (2, 4)) (t.deque())
          intercept [AssertionError] (t.deque())
          t.continue (Point.End)
          assertNone (t.deque())
        }}}

    "one point of completed work and" - {

      "no work underway and" - {

        def setup() = {
          implicit val kit = StoreTestKit.random()
          import kit._
          implicit val cluster = new StubPeer (1)
          val t = new RichTracker
          t.start (moving (1, 2, 3) (1, 2, 4))
          assertTask (begin (0), 0 -> Set (2, 4)) (t.deque())
          t.continue (7)
          (cluster, t)
        }

        "rebalance is not restarted, it should continue work" in {
          implicit val (cluster, t) = setup()
          assertTask (begin (7), 0 -> Set (2, 4)) (t.deque())
          t.continue (Point.End)
          assertNone (t.deque())
        }

        "rebalance is restarted" - {

          "with all cohorts settled, it should yield no work" in {
            implicit val (cluster, t) = setup()
            t.start (settled (1, 2, 3))
            assertNone (t.deque())
          }

          "with the same cohort moving the same way, it should continue work" in {
            implicit val (cluster, t) = setup()
            t.start (moving (1, 2, 3) (1, 2, 4))
            assertTask (begin (7), 0 -> Set (2, 4)) (t.deque())
            t.continue (Point.End)
            assertNone (t.deque())
          }

          "with the same cohort moving the same way and more, it should restart work" in {
            implicit val (cluster, t) = setup()
            t.start (moving (1, 2, 3) (1, 4, 5))
            assertTask (range (0, 7), 0 -> Set (5)) (t.deque())
            intercept [AssertionError] (t.deque())
            t.continue (7)
            assertTask (begin (7), 0 -> Set (4, 5)) (t.deque())
            t.continue (Point.End)
            assertNone (t.deque())
          }

          "with the same cohort moving a different way, it should restart work" in {
            implicit val (cluster, t) = setup()
            t.start (moving (1, 2, 3) (1, 5, 6))
            assertTask (begin (0), 0 -> Set (5, 6)) (t.deque())
            t.continue (Point.End)
            assertNone (t.deque())
          }

          "with a different cohort moving, it should restart work" in {
            implicit val (cluster, t) = setup()
            t.start (
                settled (1, 2, 3),
                moving (1, 2, 3) (1, 2, 4))
            assertTask (begin (0), 1 -> Set (2, 4)) (t.deque())
            t.continue (Point.End)
            assertNone (t.deque())
          }}}}

    "twos point of completed work and" - {

      "no work underway and" - {

        def setup() = {
          implicit val kit = StoreTestKit.random()
          import kit._
          implicit val cluster = new StubPeer (1)
          val t = new RichTracker
          t.start (moving (1, 2, 3) (1, 2, 4))
          assertTask (begin (0), 0 -> Set (2, 4)) (t.deque())
          t.start (moving (1, 2, 3) (1, 4, 5))
          t.continue (7)
          assertTask (range (0, 7), 0 -> Set (5)) (t.deque())
          t.continue (3)
          (cluster, t)
        }

        "rebalance is not restarted, it should continue work" in {
          implicit val (cluster, t) = setup()
          assertTask (range (3, 7), 0 -> Set (5)) (t.deque())
          t.continue (7)
          assertTask (begin (7), 0 -> Set (4, 5)) (t.deque())
          t.continue (Point.End)
          assertNone (t.deque())
        }

        "rebalance is restarted" - {

          "with all cohorts settled, it should yield no work" in {
            implicit val (cluster, t) = setup()
            t.start (settled (1, 2, 3))
            assertNone (t.deque())
          }

          "with the same cohort moving the same way, it should continue work" in {
            implicit val (cluster, t) = setup()
            t.start (moving (1, 2, 3) (1, 4, 5))
            assertTask (range (3, 7), 0 -> Set (5)) (t.deque())
            t.continue (7)
            assertTask (begin (7), 0 -> Set (4, 5)) (t.deque())
            t.continue (Point.End)
            assertNone (t.deque())
          }

          "with the same cohort moving a different way, it should restart work" in {
            implicit val (cluster, t) = setup()
            t.start (moving (1, 2, 3) (1, 6, 7))
            assertTask (begin (0), 0 -> Set (6, 7)) (t.deque())
            t.continue (Point.End)
            assertNone (t.deque())
          }

          "with a different cohort moving, it should restart work" in {
            implicit val (cluster, t) = setup()
            t.start (
                settled (1, 2, 3),
                moving (1, 2, 3) (1, 2, 4))
            assertTask (begin (0), 1 -> Set (2, 4)) (t.deque())
            t.continue (Point.End)
            assertNone (t.deque())
          }}}}}}
