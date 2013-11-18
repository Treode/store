package com.treode.store.local

import scala.util.Random

import com.treode.store.{Bytes, Fruits, ReadOp, TableId, WriteOp}
import org.scalatest.FreeSpec

import Fruits.Apple
import LocalTimedTestTools._
import WriteOp._

trait TimedTableBehaviors {
  this: FreeSpec =>

  private val One = Bytes (1)
  private val Two = Bytes (2)

  def aTimedTable (kit: TestableLocalKit) = {

    "when empty" - {

      "and reading should" - {

        "get Apple##0 for Apple##1" in {
          val t = kit.getTimedTable (nextTable)
          t.getAndExpect (Apple, 1) (Apple##0)
        }}

      "and writing should" - {

        "put Apple##1::1" in {
          val id = nextTable
          val t = kit.getTimedTable (id)
          t.putAndPass (Apple, 1, Some (One))
          kit.expectCells (id) (Apple##1::1)
        }}}

    "when having Apple##7::1" - {

      def newTableWithData = {
        val id = nextTable
        val t = kit.getTimedTable (id)
        t.putAndPass (Apple, 7, Some (One))
        (id, t)
      }

      "and reading should" -  {

        "find Apple##7::1 for Apple##8" in {
          val (id, t) = newTableWithData
          t.getAndExpect (Apple, 8) (Apple##7::1)
        }

        "find Apple##7::1 for Apple##7" in {
          val (id, t) = newTableWithData
          t.getAndExpect (Apple, 7) (Apple##7::1)
        }

        "find Apple##0 for Apple##6" in {
          val (id, t) = newTableWithData
          t.getAndExpect (Apple, 6) (Apple##0)
        }}

      "and writing should" -  {

        "put Apple##11::2" in {
          val (id, t) = newTableWithData
          t.putAndPass (Apple, 11, Some (Two))
          kit.expectCells (id) (Apple##11::2, Apple##7::1)
        }

        "put Apple##3::2" in {
          val (id, t) = newTableWithData
          t.putAndPass (Apple, 3, Some (Two))
          kit.expectCells (id) (Apple##7::1, Apple##3::2)
        }}}

    "when having Apple##14::2 and Apple##7::1 should" -  {

      val t = kit.getTimedTable (nextTable)
      t.putAndPass (Apple, 7, Some (One))
      t.putAndPass (Apple, 14, Some (Two))

      "find Apple##14::2 for Apple##15" in {
        t.getAndExpect (Apple, 15) (Apple##14::2)
      }

      "find Apple##14::2 for Apple##14" in {
        t.getAndExpect (Apple, 14) (Apple##14::2)
      }

      "find Apple##7::1 for Apple##13" in {
        t.getAndExpect (Apple, 13) (Apple##7::1)
      }

      "find Apple##7::1 for Apple##8" in {
        t.getAndExpect (Apple, 8) (Apple##7::1)
      }

      "find Apple##7::1 for Apple##7" in {
        t.getAndExpect (Apple, 7) (Apple##7::1)
      }

      "find Apple##0 for Apple##6" in {
        t.getAndExpect (Apple, 6) (Apple##0)
      }}}}
