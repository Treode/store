package com.treode.store.local

import scala.util.Random

import com.treode.store.{Bytes, Fruits, ReadOp, TableId, WriteOp}
import org.scalatest.FreeSpec

import Fruits.Apple
import WriteOp._

trait TimedTableBehaviors extends TimedTestTools {
  this: FreeSpec =>

  private val One = Bytes (1)
  private val Two = Bytes (2)

  private def nextTable = TableId (Random.nextLong)

  private def Get (id: TableId, key: Bytes): ReadOp =
    ReadOp (id, key)

  private def expectCells (cs: TimedCell*) (actual: TestableTimedTable) =
    expectResult (cs) (actual.toSeq)

  def aTimedTable (s: TestableLocalStore) = {

    "when empty" - {

      "and reading" - {

        "find 0::None for Apple##1" in {
          val t = nextTable
          s.readAndExpect (1, Get (t, Apple)) (0::None)
        }}

      "and a write commits should" - {

        "allow create Apple::1 at ts=0" in {
          val t = nextTable
          val ts = s.prepareAndCommit (0, Create (t, Apple, One))
          expectCells (Apple##ts::1) (s.table (t))
        }

        "allow hold Apple at ts=0" in {
          val t = nextTable
          s.prepareAndCommit (0, Hold (t, Apple))
          expectCells () (s.table (t))
        }

        "allow update Apple::1 at ts=0" in {
          val t = nextTable
          val ts = s.prepareAndCommit (0, Update (t, Apple, One))
          expectCells (Apple##ts::1) (s.table (t))
        }

        "allow delete Apple at ts=0" in {
          val t = nextTable
          val ts = s.prepareAndCommit (0, Delete (t, Apple))
          expectCells (Apple##ts) (s.table (t))
        }}

      "and a write aborts should" - {

        "allow and ignore create Apple::1 at ts=0" in {
          val t = nextTable
          s.prepareAndAbort (0, Create (t, Apple, One))
          expectCells () (s.table (t))
        }

        "allow and ignore hold Apple at ts=0" in {
          val t = nextTable
          s.prepareAndAbort (0, Hold (t, Apple))
          expectCells () (s.table (t))
        }

        "allow and ignore update Apple::1 at ts=0" in {
          val t = nextTable
          s.prepareAndAbort (0, Update (t, Apple, One))
          expectCells () (s.table (t))
        }

        "allow and ignore delete Apple at ts=0" in {
          val t = nextTable
          s.prepareAndAbort (0, Delete (t, Apple))
          expectCells () (s.table (t))
        }}}

    "when having Apple##ts::1" - {

      def newTableWithData = {
        val t = nextTable
        val ts = s.prepareAndCommit (0, Create (t, Apple, One))
        (t, ts)
      }

      "and reading" -  {

        "find ts::1 for Apple##ts+1" in {
          val (t, ts) = newTableWithData
          s.readAndExpect (ts+1, Get (t, Apple)) (ts::1)
        }

        "find ts::1 for Apple##ts" in {
          val (t, ts) = newTableWithData
          s.readAndExpect (ts, Get (t, Apple)) (ts::1)
        }

        "find 0::None for Apple##ts-1" in {
          val (t, ts) = newTableWithData
          s.readAndExpect (ts-1, Get (t, Apple)) (0::None)
        }

        "reject create Apple##ts-1" in {
          val (t, ts) = newTableWithData
          s.prepareExpectCollisions (ts-1, Create (t, Apple, One)) (0)
        }

        "reject hold Apple##ts-1" in {
          val (t, ts) = newTableWithData
          s.prepareExpectAdvance (ts-1, Hold (t, Apple))
        }

        "reject update Apple##ts-1" in {
          val (t, ts) = newTableWithData
          s.prepareExpectAdvance (ts-1, Update (t, Apple, One))
        }

        "reject delete Apple##ts-1" in {
          val (t, ts) = newTableWithData
          s.prepareExpectAdvance (ts-1, Delete (t, Apple))
        }}

      "and a write commits should" -  {

        "allow hold Apple at ts+1" in {
          val (t, ts) = newTableWithData
          s.prepareAndCommit (ts+1, Hold (t, Apple))
          expectCells (Apple##ts::1) (s.table (t))
        }

        "allow hold Apple at ts" in {
          val (t, ts) = newTableWithData
          s.prepareAndCommit (ts, Hold (t, Apple))
          expectCells (Apple##ts::1) (s.table (t))
        }

        "allow update Apple::2 at ts+1" in {
          val (t, ts1) = newTableWithData
          val ts2 = s.prepareAndCommit (ts1+1, Update (t, Apple, Two))
          expectCells (Apple##ts2::2, Apple##ts1::1) (s.table (t))
        }

        "allow update Apple::2 at ts" in {
          val (t, ts1) = newTableWithData
          val ts2 = s.prepareAndCommit (ts1, Update (t, Apple, Two))
          expectCells (Apple##ts2::2, Apple##ts1::1) (s.table (t))
        }

        "allow update Apple::1 at ts+1" in {
          val (t, ts1) = newTableWithData
          val ts2 = s.prepareAndCommit (ts1+1, Update (t, Apple, One))
          expectCells (Apple##ts2::1, Apple##ts1::1) (s.table (t))
        }

        "allow update Apple::1 at ts" in {
          val (t, ts1) = newTableWithData
          val ts2 = s.prepareAndCommit (ts1, Update (t, Apple, One))
          expectCells (Apple##ts2::1, Apple##ts1::1) (s.table (t))
        }

        "allow delete Apple at ts+1" in {
          val (t, ts1) = newTableWithData
          val ts2 = s.prepareAndCommit (ts1+1, Delete (t, Apple))
          expectCells (Apple##ts2, Apple##ts1::1) (s.table (t))
        }

        "allow delete Apple at ts" in {
          val (t, ts1) = newTableWithData
          val ts2 = s.prepareAndCommit (ts1, Delete (t, Apple))
          expectCells (Apple##ts2, Apple##ts1::1) (s.table (t))
        }}

      "and a write aborts should" -  {

        "ignore update Apple::2 at ts+1" in {
          val (t, ts1) = newTableWithData
          s.prepareAndAbort (ts1+1, Update (t, Apple, Two))
          expectCells (Apple##ts1::1) (s.table (t))
        }

        "ignore delete Apple at ts+1" in {
          val (t, ts1) = newTableWithData
          s.prepareAndAbort (ts1+1, Delete (t, Apple))
          expectCells (Apple##ts1::1) (s.table (t))
        }}}

    "when having Apple##ts2::2 and Apple##ts1::1 should" -  {

      val t = nextTable
      val ts1 = s.prepareAndCommit (0, Create (t, Apple, One))
      val ts2 = s.prepareAndCommit (ts1, Update (t, Apple, Two))

      "find ts2::2 for Apple##ts2+1" in {
        s.readAndExpect (ts2+1, Get (t, Apple)) (ts2::2)
      }

      "find ts2::2 for Apple##ts2" in {
        s.readAndExpect (ts2, Get (t, Apple)) (ts2::2)
      }

      "find ts1::1 for Apple##ts2-1" in {
        s.readAndExpect (ts2-1, Get (t, Apple)) (ts1::1)
      }

      "find ts1::1 for Apple##ts1+1" in {
        s.readAndExpect (ts1+1, Get (t, Apple)) (ts1::1)
      }

      "find ts1::1 for Apple##ts1" in {
        s.readAndExpect (ts1, Get (t, Apple)) (ts1::1)
      }

      "find 0::None for Apple##ts1-1" in {
        s.readAndExpect (ts1-1, Get (t, Apple)) (0::None)
      }}}}
