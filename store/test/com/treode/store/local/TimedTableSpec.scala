package com.treode.store.local

import com.treode.store.{Bytes, Cardinals, Fruits, WriteOp}
import com.treode.store.local.temp.TestableTempKit
import org.scalatest.FreeSpec

import Cardinals.{One, Two}
import Fruits.Apple
import LocalTimedTestTools._
import WriteOp._

class TimedTableSpec extends FreeSpec {

  private val kit = new TestableTempKit (2)

  "A TimedTable" - {

    "when empty" - {

    "and reading" - {

      "find 0::None for Apple##1" in {
        val t = nextTable
        kit.readAndExpect (1, Get (t, Apple)) (0::None)
      }}

    "and a write commits should" - {

      "allow create Apple::One at ts=0" in {
        val t = nextTable
        val ts = kit.prepareAndCommit (0, Create (t, Apple, One))
        kit.expectCells (t) (Apple##ts::One)
      }

      "allow hold Apple at ts=0" in {
        val t = nextTable
        kit.prepareAndCommit (0, Hold (t, Apple))
        kit.expectCells (t) ()
      }

      "allow update Apple::One at ts=0" in {
        val t = nextTable
        val ts = kit.prepareAndCommit (0, Update (t, Apple, One))
        kit.expectCells (t) (Apple##ts::One)
      }

      "allow delete Apple at ts=0" in {
        val t = nextTable
        val ts = kit.prepareAndCommit (0, Delete (t, Apple))
        kit.expectCells (t) (Apple##ts)
      }}

    "and a write aborts should" - {

      "allow and ignore create Apple::One at ts=0" in {
        val t = nextTable
        kit.prepareAndAbort (0, Create (t, Apple, One))
        kit.expectCells (t) ()
      }

      "allow and ignore hold Apple at ts=0" in {
        val t = nextTable
        kit.prepareAndAbort (0, Hold (t, Apple))
        kit.expectCells (t) ()
      }

      "allow and ignore update Apple::One at ts=0" in {
        val t = nextTable
        kit.prepareAndAbort (0, Update (t, Apple, One))
        kit.expectCells (t) ()
      }

      "allow and ignore delete Apple at ts=0" in {
        val t = nextTable
        kit.prepareAndAbort (0, Delete (t, Apple))
        kit.expectCells (t) ()
      }}}

  "when having Apple##ts::One" - {

    def newTableWithData = {
      val t = nextTable
      val ts = kit.prepareAndCommit (0, Create (t, Apple, One))
      (t, ts)
    }

    "and reading" -  {

      "find ts::One for Apple##ts+1" in {
        val (t, ts) = newTableWithData
        kit.readAndExpect (ts+1, Get (t, Apple)) (ts::One)
      }

      "find ts::One for Apple##ts" in {
        val (t, ts) = newTableWithData
        kit.readAndExpect (ts, Get (t, Apple)) (ts::One)
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
        kit.expectCells (t) (Apple##ts::One)
      }

      "allow hold Apple at ts" in {
        val (t, ts) = newTableWithData
        kit.prepareAndCommit (ts, Hold (t, Apple))
        kit.expectCells (t) (Apple##ts::One)
      }

      "allow update Apple::Two at ts+1" taggedAs (com.treode.store.LargeTest) in {
        val (t, ts1) = newTableWithData
        val ts2 = kit.prepareAndCommit (ts1+1, Update (t, Apple, Two))
        kit.expectCells (t) (Apple##ts2::Two, Apple##ts1::One)
      }

      "allow update Apple::Two at ts" in {
        val (t, ts1) = newTableWithData
        val ts2 = kit.prepareAndCommit (ts1, Update (t, Apple, Two))
        kit.expectCells (t) (Apple##ts2::Two, Apple##ts1::One)
      }

      "allow update Apple::One at ts+1" in {
        val (t, ts1) = newTableWithData
        val ts2 = kit.prepareAndCommit (ts1+1, Update (t, Apple, One))
        kit.expectCells (t) (Apple##ts2::One, Apple##ts1::One)
      }

      "allow update Apple::One at ts" in {
        val (t, ts1) = newTableWithData
        val ts2 = kit.prepareAndCommit (ts1, Update (t, Apple, One))
        kit.expectCells (t) (Apple##ts2::One, Apple##ts1::One)
      }

      "allow delete Apple at ts+1" in {
        val (t, ts1) = newTableWithData
        val ts2 = kit.prepareAndCommit (ts1+1, Delete (t, Apple))
        kit.expectCells (t) (Apple##ts2, Apple##ts1::One)
      }

      "allow delete Apple at ts" in {
        val (t, ts1) = newTableWithData
        val ts2 = kit.prepareAndCommit (ts1, Delete (t, Apple))
        kit.expectCells (t) (Apple##ts2, Apple##ts1::One)
      }}

    "and a write aborts should" -  {

      "ignore update Apple::Two at ts+1" in {
        val (t, ts1) = newTableWithData
        kit.prepareAndAbort (ts1+1, Update (t, Apple, Two))
        kit.expectCells (t) (Apple##ts1::One)
      }

      "ignore delete Apple at ts+1" in {
        val (t, ts1) = newTableWithData
        kit.prepareAndAbort (ts1+1, Delete (t, Apple))
        kit.expectCells (t) (Apple##ts1::One)
      }}}

  "when having Apple##ts2::Two and Apple##ts1::One should" -  {

    val t = nextTable
    val ts1 = kit.prepareAndCommit (0, Create (t, Apple, One))
    val ts2 = kit.prepareAndCommit (ts1, Update (t, Apple, Two))

    "find ts2::Two for Apple##ts2+1" in {
      kit.readAndExpect (ts2+1, Get (t, Apple)) (ts2::Two)
    }

    "find ts2::Two for Apple##ts2" in {
      kit.readAndExpect (ts2, Get (t, Apple)) (ts2::Two)
    }

    "find ts1::One for Apple##ts2-1" in {
      kit.readAndExpect (ts2-1, Get (t, Apple)) (ts1::One)
    }

    "find ts1::One for Apple##ts1+1" in {
      kit.readAndExpect (ts1+1, Get (t, Apple)) (ts1::One)
    }

    "find ts1::One for Apple##ts1" in {
      kit.readAndExpect (ts1, Get (t, Apple)) (ts1::One)
    }

    "find 0::None for Apple##ts1-1" in {
      kit.readAndExpect (ts1-1, Get (t, Apple)) (0::None)
    }}}}
