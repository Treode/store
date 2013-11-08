package com.treode.store.local

import com.treode.pickle.Picklers
import com.treode.store._
import org.scalamock.scalatest.MockFactory
import org.scalatest.FreeSpec

import Fruits.Apple
import WriteOp._

class TempTimedTableSpec extends FreeSpec with TimedTestTools {

  private val One = Bytes (1)
  private val Two = Bytes (2)

  private def Get (id: Int, key: Bytes): ReadOp =
    ReadOp (id, key)

  private def expectCells (cs: TimedCell*) (actual: TestableTempTimedTable) =
    expectResult (cs) (actual.toSeq)

  "The TempTable" - {

    "when empty" - {

      def newStore = new TestableTempLocalStore (4)

      "and reading" - {

        "find 0::None for Apple##1" in {
          val s = newStore
          s.readAndExpect (1, Get (1, Apple)) (0::None)
        }}

      "and a write commits should" - {

        "allow and perform create Apple::1 at t=0" in {
          val s = newStore
          s.writeExpectApply (0, Create (1, Apple, One)) (_.commit (1))
          expectCells (Apple##1::1) (s.table (1))
        }

        "allow and ignore hold Apple at t=0" in {
          val s = newStore
          s.writeExpectApply (0, Hold (1, Apple)) (_.commit (1))
          expectCells () (s.table (1))
        }

        "allow and perform update Apple::1 at t=0" in {
          val s = newStore
          s.writeExpectApply (0, Update (1, Apple, One)) (_.commit (1))
          expectCells (Apple##1::1) (s.table (1))
        }

        "allow and ignore delete Apple at t=0" in {
          val s = newStore
          s.writeExpectApply (0, Delete (1, Apple)) (_.commit (1))
          expectCells () (s.table (1))
        }}

      "and a write aborts should" - {

        "allow and ignore create Apple::1 at t=0" in {
          val s = newStore
          s.writeExpectApply (0, Create (1, Apple, One)) (_.abort())
          expectCells () (s.table (1))
        }

        "allow and ignore hold Apple at t=0" in {
          val s = newStore
          s.writeExpectApply (0, Hold (1, Apple)) (_.abort())
          expectCells () (s.table (1))
        }

        "allow and ignore update Apple::1 at t=0" in {
          val s = newStore
          s.writeExpectApply (0, Update (1, Apple, One)) (_.abort())
          expectCells () (s.table (1))
        }

        "allow and ignore delete Apple at t=0" in {
          val s = newStore
          s.writeExpectApply (0, Delete (1, Apple)) (_.abort())
          expectCells () (s.table (1))
        }}}

    "when having Apple##7::1" - {

      def newStore = {
        val s = new TestableTempLocalStore (4)
        s.writeExpectApply (0, Create (1, Apple, One)) (_.commit (7))
        s
      }

      "and reading" -  {

        "find 7::1 for Apple##8" in {
          val s = newStore
          s.readAndExpect (8, Get (1, Apple)) (7::1)
        }

        "find 7::1 for Apple##7" in {
          val s = newStore
          s.readAndExpect (7, Get (1, Apple)) (7::1)
        }

        "find 0::None for Apple##6" in {
          val s = newStore
          s.readAndExpect (6, Get (1, Apple)) (0::None)
        }

        "reject create Apple##6" in {
          val s = newStore
          s.writeExpectConflicts (6, Create (1, Apple, One)) (0)
        }

        "reject hold Apple##6" in {
          val s = newStore
          s.writeExpectAdvance (6, Hold (1, Apple))
        }

        "reject update Apple##6" in {
          val s = newStore
          s.writeExpectAdvance (6, Update (1, Apple, One))
        }

        "reject delete Apple##6" in {
          val s = newStore
          s.writeExpectAdvance (6, Delete (1, Apple))
        }}

      "and a write commits should" -  {

        "allow and ignore hold Apple at t=8" in {
          val s = newStore
          s.writeExpectApply (8, Hold (1, Apple)) (_.commit (14))
          expectCells (Apple##7::1) (s.table (1))
        }

        "allow and ignore hold Apple at t=7" in {
          val s = newStore
          s.writeExpectApply (7, Hold (1, Apple)) (_.commit (14))
          expectCells (Apple##7::1) (s.table (1))
        }

        "allow and perform update Apple::2 at t=8" in {
          val s = newStore
          s.writeExpectApply (8, Update (1, Apple, Two)) (_.commit (14))
          expectCells (Apple##14::2, Apple##7::1) (s.table (1))
        }

        "allow and perform update Apple::2 at t=7" in {
          val s = newStore
          s.writeExpectApply (7, Update (1, Apple, Two)) (_.commit (14))
          expectCells (Apple##14::2, Apple##7::1) (s.table (1))
        }

        "allow and ignore update Apple::1 at t=8" in {
          val s = newStore
          s.writeExpectApply (8, Update (1, Apple, One)) (_.commit (14))
          expectCells (Apple##7::1) (s.table (1))
        }

        "allow and ignore update Apple::1 at t=7" in {
          val s = newStore
          s.writeExpectApply (7, Update (1, Apple, One)) (_.commit (14))
          expectCells (Apple##7::1) (s.table (1))
        }

        "allow and perform delete Apple at t=8" in {
          val s = newStore
          s.writeExpectApply (8, Delete (1, Apple)) (_.commit (14))
          expectCells (Apple##14, Apple##7::1) (s.table (1))
        }

        "allow and perform delete Apple at t=7" in {
          val s = newStore
          s.writeExpectApply (7, Delete (1, Apple)) (_.commit (14))
          expectCells (Apple##14, Apple##7::1) (s.table (1))
        }}

      "and a write aborts should" -  {

        "allow and ignore update Apple::2 at t=8" in {
          val s = newStore
          s.writeExpectApply (8, Update (1, Apple, Two)) (_.commit (14))
          expectCells (Apple##14::2, Apple##7::1) (s.table (1))
        }

        "allow and perform delete Apple at t=8" in {
          val s = newStore
          s.writeExpectApply (8, Delete (1, Apple)) (_.commit (14))
          expectCells (Apple##14, Apple##7::1) (s.table (1))
        }}}

    "when having Apple##14::2 and Apple##7::1 should" -  {

      val s = new TestableTempLocalStore (4)
      s.writeExpectApply (0, Create (1, Apple, One)) (_.commit (7))
      s.writeExpectApply (7, Update (1, Apple, Two)) (_.commit (14))

      "find 14::2 for Apple##15" in {
        s.readAndExpect (15, Get (1, Apple)) (14::2)
      }

      "find 14::2 for Apple##14" in {
        s.readAndExpect (14, Get (1, Apple)) (14::2)
      }

      "find 7::1 for Apple##13" in {
        s.readAndExpect (13, Get (1, Apple)) (7::1)
      }

      "find 7::1 for Apple##8" in {
        s.readAndExpect (8, Get (1, Apple)) (7::1)
      }

      "find 7::1 for Apple##7" in {
        s.readAndExpect (7, Get (1, Apple)) (7::1)
      }

      "find 0::None for Apple##6" in {
        s.readAndExpect (6, Get (1, Apple)) (0::None)
      }}}}
