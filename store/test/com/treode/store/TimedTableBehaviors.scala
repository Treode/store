package com.treode.store

import scala.util.Random
import org.scalatest.FreeSpec

import Cardinals.{One, Two}
import Fruits.Apple
import TimedTestTools._
import WriteOp._

trait TimedTableBehaviors {
  this: FreeSpec =>

  def aTimedTable (kit: TestableLocalKit) = {

    "when empty" - {

      "and reading should" - {

        "get Apple##0 for Apple##1" in {
          val t = kit.getTimedTable (nextTable)
          t.getAndExpect (Apple, 1) (Apple##0)
        }}

      "and writing should" - {

        "put Apple##1::One" in {
          val id = nextTable
          val t = kit.getTimedTable (id)
          t.putAndPass (Apple, 1, Some (One))
          kit.expectCells (id) (Apple##1::One)
        }}}

    "when having Apple##7::One" - {

      def newTableWithData = {
        val id = nextTable
        val t = kit.getTimedTable (id)
        t.putAndPass (Apple, 7, Some (One))
        (id, t)
      }

      "and reading should" -  {

        "find Apple##7::One for Apple##8" in {
          val (id, t) = newTableWithData
          t.getAndExpect (Apple, 8) (Apple##7::One)
        }

        "find Apple##7::One for Apple##7" in {
          val (id, t) = newTableWithData
          t.getAndExpect (Apple, 7) (Apple##7::One)
        }

        "find Apple##0 for Apple##6" in {
          val (id, t) = newTableWithData
          t.getAndExpect (Apple, 6) (Apple##0)
        }}

      "and writing should" -  {

        "put Apple##11::Two" in {
          val (id, t) = newTableWithData
          t.putAndPass (Apple, 11, Some (Two))
          kit.expectCells (id) (Apple##11::Two, Apple##7::One)
        }

        "put Apple##3::Two" in {
          val (id, t) = newTableWithData
          t.putAndPass (Apple, 3, Some (Two))
          kit.expectCells (id) (Apple##7::One, Apple##3::Two)
        }}}

    "when having Apple##14::Two and Apple##7::One should" -  {

      val t = kit.getTimedTable (nextTable)
      t.putAndPass (Apple, 7, Some (One))
      t.putAndPass (Apple, 14, Some (Two))

      "find Apple##14::Two for Apple##15" in {
        t.getAndExpect (Apple, 15) (Apple##14::Two)
      }

      "find Apple##14::Two for Apple##14" in {
        t.getAndExpect (Apple, 14) (Apple##14::Two)
      }

      "find Apple##7::One for Apple##13" in {
        t.getAndExpect (Apple, 13) (Apple##7::One)
      }

      "find Apple##7::One for Apple##8" in {
        t.getAndExpect (Apple, 8) (Apple##7::One)
      }

      "find Apple##7::One for Apple##7" in {
        t.getAndExpect (Apple, 7) (Apple##7::One)
      }

      "find Apple##0 for Apple##6" in {
        t.getAndExpect (Apple, 6) (Apple##0)
      }}}}
