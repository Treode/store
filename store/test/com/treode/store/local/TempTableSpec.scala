package com.treode.store.local

import com.treode.pickle.Picklers
import com.treode.store._
import org.scalamock.scalatest.MockFactory
import org.scalatest.WordSpec

import Fruits.Apple
import WriteOp._

class TempTableSpec extends WordSpec {

  private val One = Bytes ("one")
  private val Two = Bytes ("two")

  implicit class RichBytes (v: Bytes) {
    def ## (time: Int) = Cell (v, time, None)
    def :: (cell: Cell): Cell = Cell (cell.key, cell.time, Some (v))
    def :: (time: Int): Value = Value (time, Some (v))
  }

  implicit class RichOption (v: Option [Bytes]) {
    def :: (time: Int): Value = Value (time, v)
  }

  def Get (id: Int, key: Bytes): ReadOp =
    ReadOp (id, key)

  def expectCells [T] (cs: Cell*) (actual: TestableTempTable) =
    expectResult (cs) (actual.toSeq)

  "The TempTable" when {

    "empty and the transaction" when {

      def newStore = new TestableTempStore

      "not relevant" should {

        "find 0::None for Apple##1" in {
          val s = newStore
          s.readAndExpect (1, Get (1, Apple)) (0::None)
        }}

      "commited" should {

        "allow and perform create Apple::One at t=0" in {
          val s = newStore
          s.writeExpectApply (0, Create (1, Apple, One)) (_.commit (1))
          expectCells (Apple##1::One) (s.table (1))
        }

        "allow and ignore hold Apple at t=0" in {
          val s = newStore
          s.writeExpectApply (0, Hold (1, Apple)) (_.commit (1))
          expectCells () (s.table (1))
        }

        "allow and perform update Apple::One at t=0" in {
          val s = newStore
          s.writeExpectApply (0, Update (1, Apple, One)) (_.commit (1))
          expectCells (Apple##1::One) (s.table (1))
        }

        "allow and ignore delete Apple at t=0" in {
          val s = newStore
          s.writeExpectApply (0, Delete (1, Apple)) (_.commit (1))
          expectCells () (s.table (1))
        }}

      "aborted" should {

        "allow and ignore create Apple::One at t=0" in {
          val s = newStore
          s.writeExpectApply (0, Create (1, Apple, One)) (_.abort())
          expectCells () (s.table (1))
        }

        "allow and ignore hold Apple at t=0" in {
          val s = newStore
          s.writeExpectApply (0, Hold (1, Apple)) (_.abort())
          expectCells () (s.table (1))
        }

        "allow and ignore update Apple::One at t=0" in {
          val s = newStore
          s.writeExpectApply (0, Update (1, Apple, One)) (_.abort())
          expectCells () (s.table (1))
        }

        "allow and ignore delete Apple at t=0" in {
          val s = newStore
          s.writeExpectApply (0, Delete (1, Apple)) (_.abort())
          expectCells () (s.table (1))
        }}}

    "having Apple##7::One and the transaction" when {

      def newStore = {
        val s = new TestableTempStore
        s.writeExpectApply (0, Create (1, Apple, One)) (_.commit (7))
        s
      }

      "not relevant" should {

        "find 7::One for Apple##8" in {
          val s = newStore
          s.readAndExpect (8, Get (1, Apple)) (7::One)
        }

        "find 7::One for Apple##7" in {
          val s = newStore
          s.readAndExpect (7, Get (1, Apple)) (7::One)
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

      "commited" should {

        "allow and ignore hold Apple at t=8" in {
          val s = newStore
          s.writeExpectApply (8, Hold (1, Apple)) (_.commit (14))
          expectCells (Apple##7::One) (s.table (1))
        }

        "allow and ignore hold Apple at t=7" in {
          val s = newStore
          s.writeExpectApply (7, Hold (1, Apple)) (_.commit (14))
          expectCells (Apple##7::One) (s.table (1))
        }

        "allow and perform update Apple::Two at t=8" in {
          val s = newStore
          s.writeExpectApply (8, Update (1, Apple, Two)) (_.commit (14))
          expectCells (Apple##14::Two, Apple##7::One) (s.table (1))
        }

        "allow and perform update Apple::Two at t=7" in {
          val s = newStore
          s.writeExpectApply (7, Update (1, Apple, Two)) (_.commit (14))
          expectCells (Apple##14::Two, Apple##7::One) (s.table (1))
        }

        "allow and ignore update Apple::One at t=8" in {
          val s = newStore
          s.writeExpectApply (8, Update (1, Apple, One)) (_.commit (14))
          expectCells (Apple##7::One) (s.table (1))
        }

        "allow and ignore update Apple::One at t=7" in {
          val s = newStore
          s.writeExpectApply (7, Update (1, Apple, One)) (_.commit (14))
          expectCells (Apple##7::One) (s.table (1))
        }

        "allow and perform delete Apple at t=8" in {
          val s = newStore
          s.writeExpectApply (8, Delete (1, Apple)) (_.commit (14))
          expectCells (Apple##14, Apple##7::One) (s.table (1))
        }

        "allow and perform delete Apple at t=7" in {
          val s = newStore
          s.writeExpectApply (7, Delete (1, Apple)) (_.commit (14))
          expectCells (Apple##14, Apple##7::One) (s.table (1))
        }}

      "aborted" should {

        "allow and ignore update Apple::Two at t=8" in {
          val s = newStore
          s.writeExpectApply (8, Update (1, Apple, Two)) (_.commit (14))
          expectCells (Apple##14::Two, Apple##7::One) (s.table (1))
        }

        "allow and perform delete Apple at t=8" in {
          val s = newStore
          s.writeExpectApply (8, Delete (1, Apple)) (_.commit (14))
          expectCells (Apple##14, Apple##7::One) (s.table (1))
        }}}

    "having Apple##14::Two and Apple##7::One" should {

      val s = new TestableTempStore
      s.writeExpectApply (0, Create (1, Apple, One)) (_.commit (7))
      s.writeExpectApply (7, Update (1, Apple, Two)) (_.commit (14))

      "find 14::Two for Apple##15" in {
        s.readAndExpect (15, Get (1, Apple)) (14::Two)
      }

      "find 14::Two for Apple##14" in {
        s.readAndExpect (14, Get (1, Apple)) (14::Two)
      }

      "find 7::One for Apple##13" in {
        s.readAndExpect (13, Get (1, Apple)) (7::One)
      }

      "find 7::One for Apple##8" in {
        s.readAndExpect (8, Get (1, Apple)) (7::One)
      }

      "find 7::One for Apple##7" in {
        s.readAndExpect (7, Get (1, Apple)) (7::One)
      }

      "find 0::None for Apple##6" in {
        s.readAndExpect (6, Get (1, Apple)) (0::None)
      }}}}
