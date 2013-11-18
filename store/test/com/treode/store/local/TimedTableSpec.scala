package com.treode.store.local

import com.treode.store.{Bytes, Fruits, WriteOp}
import com.treode.store.local.temp.TestableTempKit
import org.scalatest.FreeSpec

import Fruits.Apple
import LocalTimedTestTools._
import WriteOp._

class TimedTableSpec extends FreeSpec {

  private val One = Bytes (1)
  private val Two = Bytes (2)

  private val kit = new TestableTempKit (2)

  "A TimedTable" - {

    "when empty" - {

    "and reading" - {

      "find 0::None for Apple##1" in {
        val t = nextTable
        kit.readAndExpect (1, Get (t, Apple)) (0::None)
      }}

    "and a write commits should" - {

      "allow create Apple::1 at ts=0" in {
        val t = nextTable
        val ts = kit.prepareAndCommit (0, Create (t, Apple, One))
        expectCells (Apple##ts::1) (kit.getTimedTable (t))
      }

      "allow hold Apple at ts=0" in {
        val t = nextTable
        kit.prepareAndCommit (0, Hold (t, Apple))
        expectCells () (kit.getTimedTable (t))
      }

      "allow update Apple::1 at ts=0" in {
        val t = nextTable
        val ts = kit.prepareAndCommit (0, Update (t, Apple, One))
        expectCells (Apple##ts::1) (kit.getTimedTable (t))
      }

      "allow delete Apple at ts=0" in {
        val t = nextTable
        val ts = kit.prepareAndCommit (0, Delete (t, Apple))
        expectCells (Apple##ts) (kit.getTimedTable (t))
      }}

    "and a write aborts should" - {

      "allow and ignore create Apple::1 at ts=0" in {
        val t = nextTable
        kit.prepareAndAbort (0, Create (t, Apple, One))
        expectCells () (kit.getTimedTable (t))
      }

      "allow and ignore hold Apple at ts=0" in {
        val t = nextTable
        kit.prepareAndAbort (0, Hold (t, Apple))
        expectCells () (kit.getTimedTable (t))
      }

      "allow and ignore update Apple::1 at ts=0" in {
        val t = nextTable
        kit.prepareAndAbort (0, Update (t, Apple, One))
        expectCells () (kit.getTimedTable (t))
      }

      "allow and ignore delete Apple at ts=0" in {
        val t = nextTable
        kit.prepareAndAbort (0, Delete (t, Apple))
        expectCells () (kit.getTimedTable (t))
      }}}

  "when having Apple##ts::1" - {

    def newTableWithData = {
      val t = nextTable
      val ts = kit.prepareAndCommit (0, Create (t, Apple, One))
      (t, ts)
    }

    "and reading" -  {

      "find ts::1 for Apple##ts+1" in {
        val (t, ts) = newTableWithData
        kit.readAndExpect (ts+1, Get (t, Apple)) (ts::1)
      }

      "find ts::1 for Apple##ts" in {
        val (t, ts) = newTableWithData
        kit.readAndExpect (ts, Get (t, Apple)) (ts::1)
      }

      "find 0::None for Apple##ts-1" in {
        val (t, ts) = newTableWithData
        kit.readAndExpect (ts-1, Get (t, Apple)) (0::None)
      }

      "reject create Apple##ts-1" in {
        val (t, ts) = newTableWithData
        kit.prepareExpectCollisions (ts-1, Create (t, Apple, One)) (0)
      }

      "reject hold Apple##ts-1" in {
        val (t, ts) = newTableWithData
        kit.prepareExpectAdvance (ts-1, Hold (t, Apple))
      }

      "reject update Apple##ts-1" in {
        val (t, ts) = newTableWithData
        kit.prepareExpectAdvance (ts-1, Update (t, Apple, One))
      }

      "reject delete Apple##ts-1" in {
        val (t, ts) = newTableWithData
        kit.prepareExpectAdvance (ts-1, Delete (t, Apple))
      }}

    "and a write commits should" -  {

      "allow hold Apple at ts+1" in {
        val (t, ts) = newTableWithData
        kit.prepareAndCommit (ts+1, Hold (t, Apple))
        expectCells (Apple##ts::1) (kit.getTimedTable (t))
      }

      "allow hold Apple at ts" in {
        val (t, ts) = newTableWithData
        kit.prepareAndCommit (ts, Hold (t, Apple))
        expectCells (Apple##ts::1) (kit.getTimedTable (t))
      }

      "allow update Apple::2 at ts+1" in {
        val (t, ts1) = newTableWithData
        val ts2 = kit.prepareAndCommit (ts1+1, Update (t, Apple, Two))
        expectCells (Apple##ts2::2, Apple##ts1::1) (kit.getTimedTable (t))
      }

      "allow update Apple::2 at ts" in {
        val (t, ts1) = newTableWithData
        val ts2 = kit.prepareAndCommit (ts1, Update (t, Apple, Two))
        expectCells (Apple##ts2::2, Apple##ts1::1) (kit.getTimedTable (t))
      }

      "allow update Apple::1 at ts+1" in {
        val (t, ts1) = newTableWithData
        val ts2 = kit.prepareAndCommit (ts1+1, Update (t, Apple, One))
        expectCells (Apple##ts2::1, Apple##ts1::1) (kit.getTimedTable (t))
      }

      "allow update Apple::1 at ts" in {
        val (t, ts1) = newTableWithData
        val ts2 = kit.prepareAndCommit (ts1, Update (t, Apple, One))
        expectCells (Apple##ts2::1, Apple##ts1::1) (kit.getTimedTable (t))
      }

      "allow delete Apple at ts+1" in {
        val (t, ts1) = newTableWithData
        val ts2 = kit.prepareAndCommit (ts1+1, Delete (t, Apple))
        expectCells (Apple##ts2, Apple##ts1::1) (kit.getTimedTable (t))
      }

      "allow delete Apple at ts" in {
        val (t, ts1) = newTableWithData
        val ts2 = kit.prepareAndCommit (ts1, Delete (t, Apple))
        expectCells (Apple##ts2, Apple##ts1::1) (kit.getTimedTable (t))
      }}

    "and a write aborts should" -  {

      "ignore update Apple::2 at ts+1" in {
        val (t, ts1) = newTableWithData
        kit.prepareAndAbort (ts1+1, Update (t, Apple, Two))
        expectCells (Apple##ts1::1) (kit.getTimedTable (t))
      }

      "ignore delete Apple at ts+1" in {
        val (t, ts1) = newTableWithData
        kit.prepareAndAbort (ts1+1, Delete (t, Apple))
        expectCells (Apple##ts1::1) (kit.getTimedTable (t))
      }}}

  "when having Apple##ts2::2 and Apple##ts1::1 should" -  {

    val t = nextTable
    val ts1 = kit.prepareAndCommit (0, Create (t, Apple, One))
    val ts2 = kit.prepareAndCommit (ts1, Update (t, Apple, Two))

    "find ts2::2 for Apple##ts2+1" in {
      kit.readAndExpect (ts2+1, Get (t, Apple)) (ts2::2)
    }

    "find ts2::2 for Apple##ts2" in {
      kit.readAndExpect (ts2, Get (t, Apple)) (ts2::2)
    }

    "find ts1::1 for Apple##ts2-1" in {
      kit.readAndExpect (ts2-1, Get (t, Apple)) (ts1::1)
    }

    "find ts1::1 for Apple##ts1+1" in {
      kit.readAndExpect (ts1+1, Get (t, Apple)) (ts1::1)
    }

    "find ts1::1 for Apple##ts1" in {
      kit.readAndExpect (ts1, Get (t, Apple)) (ts1::1)
    }

    "find 0::None for Apple##ts1-1" in {
      kit.readAndExpect (ts1-1, Get (t, Apple)) (0::None)
    }}}}
