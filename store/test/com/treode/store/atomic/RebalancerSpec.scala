package com.treode.store.atomic

import com.treode.async.stubs.StubScheduler
import com.treode.cluster.{Cluster, HostId, Peer}
import com.treode.cluster.stubs.StubCluster
import com.treode.store.{Atlas, Bytes, Cohort, StoreTestKit, StoreTestTools, TxClock}
import org.scalatest.{FreeSpec, ShouldMatchers}

import Cohort.{issuing, moving, settled}
import Rebalancer.{Point, Range, Targets, Tracker}
import StoreTestTools.{intToBytes, longToTxClock}

class RebalancerSpec extends FreeSpec with ShouldMatchers {

  private def targets (cohorts: Cohort*) (implicit cluster: Cluster): Targets =
    Targets (Atlas (cohorts.toArray, 1))

  private def begin (start: Int): Range =
    Range (Point.Middle (start, Bytes.empty, TxClock.max), Point.End)

  private def range (start: Int, end: Int): Range =
    Range (Point.Middle (start, Bytes.empty, TxClock.max), Point.Middle (end, Bytes.empty, TxClock.max))

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

    def continue (table: Int): Unit =
      tracker.continue (Point.Middle (table, Bytes.empty, TxClock.max))

    def start (cohorts: Cohort*) (implicit cluster: Cluster): Unit =
      tracker.start (targets (cohorts: _*))
  }

  "Deriving targets from cohorts should" - {

    def setup() = {
      implicit val kit = StoreTestKit.random()
      import kit._
      implicit val cluster = new StubCluster (0)
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
      assertPeers (5) (ts (2))
      assertPeers (6, 7) (ts (3))
    }}

  "Points should" - {
    import Point.{End, Middle}

    "order properly" in {
      assert (Middle (0, 0, 1) < Middle (0, 0, 0))
      assert (Middle (0, 0, 0) < Middle (0, 1, 0))
      assert (Middle (0, 0, 0) < Middle (1, 0, 0))
      assert (Middle (0, 0, 0) < End)
    }}

  "When the tracker" - {

    "no points of completed work and" - {

      "no work underway and" - {

        def setup() = {
          implicit val kit = StoreTestKit.random()
          import kit._
          implicit val cluster = new StubCluster (0)
          val tracker = new RichTracker
          (cluster, tracker)
        }

        "rebalance is not started, it should yield no work" in {
          implicit val (cluster, t) = setup()
          assertNone (t.deque())
          intercept [AssertionError] (t.continue (0))
        }

        "rebalance is started with all cohorts settled, it should yield no work" in {
          implicit val (cluster, t) = setup()
          t.start (settled (1, 2, 3))
          assertNone (t.deque())
          assertNone (t.deque())
          intercept [AssertionError] (t.continue (0))
        }

        "rebalance is started with a cohort moving, it should start work" in {
          implicit val (cluster, t) = setup()
          t.start (moving (1, 2, 3) (1, 2, 4))
          assertTask (begin (0), 0 -> Set (4)) (t.deque())
          intercept [AssertionError] (t.deque())
          t.continue (Point.End)
          assertNone (t.deque())
        }}}

    "one point of completed work and" - {

      "no work underway and" - {

        def setup() = {
          implicit val kit = StoreTestKit.random()
          import kit._
          implicit val cluster = new StubCluster (0)
          val t = new RichTracker
          t.start (moving (1, 2, 3) (1, 2, 4))
          assertTask (begin (0), 0 -> Set (4)) (t.deque())
          t.continue (7)
          (cluster, t)
        }

        "rebalance is not restarted, it should continue work" in {
          implicit val (cluster, t) = setup()
          assertTask (begin (7), 0 -> Set (4)) (t.deque())
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
            assertTask (begin (7), 0 -> Set (4)) (t.deque())
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
            t.start (moving (1, 2, 3) (1, 2, 5))
            assertTask (begin (0), 0 -> Set (5)) (t.deque())
            t.continue (Point.End)
            assertNone (t.deque())
          }

          "with a different cohort moving, it should restart work" in {
            implicit val (cluster, t) = setup()
            t.start (
                settled (1, 2, 3),
                moving (1, 2, 3) (1, 2, 4))
            assertTask (begin (0), 1 -> Set (4)) (t.deque())
            t.continue (Point.End)
            assertNone (t.deque())
          }}}}

    "twos point of completed work and" - {

      "no work underway and" - {

        def setup() = {
          implicit val kit = StoreTestKit.random()
          import kit._
          implicit val cluster = new StubCluster (0)
          val t = new RichTracker
          t.start (moving (1, 2, 3) (1, 2, 4))
          assertTask (begin (0), 0 -> Set (4)) (t.deque())
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
            t.start (moving (1, 2, 3) (1, 2, 6))
            assertTask (begin (0), 0 -> Set (6)) (t.deque())
            t.continue (Point.End)
            assertNone (t.deque())
          }

          "with a different cohort moving, it should restart work" in {
            implicit val (cluster, t) = setup()
            t.start (
                settled (1, 2, 3),
                moving (1, 2, 3) (1, 2, 4))
            assertTask (begin (0), 1 -> Set (4)) (t.deque())
            t.continue (Point.End)
            assertNone (t.deque())
          }}}}}}
