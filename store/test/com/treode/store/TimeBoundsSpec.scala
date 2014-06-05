package com.treode.store

import com.treode.async.implicits._
import com.treode.async.stubs.StubScheduler
import com.treode.async.stubs.implicits._
import org.scalatest.FreeSpec

import Fruits.{Apple, Banana}
import TimeBounds.{Recent, Between, Through}
import StoreTestTools._

class TimeBoundsSpec extends FreeSpec {

  def testStringOf (cell: Cell): String = {
    val k = cell.key.string
    val t = cell.time.time
    cell.value match {
      case Some (v) => s"$k##$t::${v.int}"
      case None => s"$k##$t::_"
    }}

  def testStringOf (cells: Seq [Cell]): String =
    cells.map (testStringOf _) .mkString ("[", ", ", "]")

  def concat [A, B] (x: (Seq [A], Seq [B]), y: (Seq [A], Seq [B])): (Seq [A], Seq [B]) =
    (x._1 ++ y._1, x._2 ++ y._2)

  def assertCells (expected: Seq [Cell]) (actual: Seq [Cell]): Unit = {
    if (expected != actual)
      fail (s"Expected ${expected map (testStringOf _)}, found ${actual map (testStringOf _)}")
  }

  "TimeBounds.Recent should" - {

    val filter = Recent (2, true)

    def test (items: (Seq [Cell], Seq [Cell])*) {
      val in = items .map (_._1) .flatten
      val out = items .map (_._2) .flatten
      s"handle ${testStringOf (in)}" in {
        implicit val scheduler = StubScheduler.random()
        assertCells (out) (filter.filter (in.iterator.async) .toSeq)
      }}

    val apple1 = (
        Seq (Apple##1::1),
        Seq (Apple##1::1))

    val apple2 = (
        Seq (Apple##2::2),
        Seq (Apple##2::2))

    val apple3 = (
        Seq (Apple##2::2, Apple##1::1),
        Seq (Apple##2::2))

    val apple4 = (
        Seq (Apple##3::3, Apple##2::2, Apple##1::1),
        Seq (Apple##2::2))

    val apple5 = (
        Seq (Apple##3::3),
        Seq ())

    val apples = Seq (apple1, apple2, apple3, apple4, apple5)

    val banana1 = (
        Seq (Banana##1::1),
        Seq (Banana##1::1))

    val banana2 = (
        Seq (Banana##2::2),
        Seq (Banana##2::2))

    val banana3 = (
        Seq (Banana##2::2, Banana##1::1),
        Seq (Banana##2::2))

    val banana4 = (
        Seq (Banana##3::3, Banana##2::2, Banana##1::1),
        Seq (Banana##2::2))

    val banana5 = (
        Seq (Banana##3::3),
        Seq ())

    val bananas = Seq (banana1, banana2, banana3, banana4, banana5)


    test ((Seq.empty, Seq.empty))
    for (a <- apples)
      test (a)
    for (a <- apples; b <- bananas)
      test (concat (a, b))
  }

  "TimeBounds.Between should" - {

    val filter = Between (3, true, 2, true)

    def test (items: (Seq [Cell], Seq [Cell])*) {
      val in = items .map (_._1) .flatten
      val out = items .map (_._2) .flatten
      s"handle ${testStringOf (in)}" in {
        implicit val scheduler = StubScheduler.random()
        assertCells (out) (filter.filter (in.iterator.async) .toSeq)
      }}

    val apple1 = (
        Seq (Apple##1::1),
        Seq ())

    val apple2 = (
        Seq (Apple##2::2),
        Seq (Apple##2::2))

    val apple3 = (
        Seq (Apple##2::2, Apple##1::1),
        Seq (Apple##2::2))

    val apple4 = (
        Seq (Apple##3::3, Apple##2::2, Apple##1::1),
        Seq (Apple##3::3, Apple##2::2))

    val apple5 = (
        Seq (Apple##4::4, Apple##3::3, Apple##2::2, Apple##1::1),
        Seq (Apple##3::3, Apple##2::2))

    val apple6 = (
        Seq (Apple##4::4, Apple##3::3, Apple##2::2),
        Seq (Apple##3::3, Apple##2::2))

    val apple7 = (
        Seq (Apple##4::4, Apple##3::3),
        Seq (Apple##3::3))

    val apples = Seq (apple1, apple2, apple3, apple4, apple5, apple6, apple7)

    test ((Seq.empty, Seq.empty))
    for (a <- apples)
      test (a)
  }

  "TimeBounds.Through should" - {

    val filter = Through (3, true, 2)

    def test (items: (Seq [Cell], Seq [Cell])*) {
      val in = items .map (_._1) .flatten
      val out = items .map (_._2) .flatten
      s"handle ${testStringOf (in)}" in {
        implicit val scheduler = StubScheduler.random()
        assertCells (out) (filter.filter (in.iterator.async) .toSeq)
      }}

    val apple1 = (
        Seq (Apple##1::1),
        Seq (Apple##1::1))

    val apple2 = (
        Seq (Apple##2::2),
        Seq (Apple##2::2))

    val apple3 = (
        Seq (Apple##2::2, Apple##1::1),
        Seq (Apple##2::2))

    val apple4 = (
        Seq (Apple##3::3, Apple##2::2, Apple##1::1),
        Seq (Apple##3::3, Apple##2::2))

    val apple5 = (
        Seq (Apple##4::4, Apple##3::3, Apple##2::2, Apple##1::1),
        Seq (Apple##3::3, Apple##2::2))

    val apple6 = (
        Seq (Apple##4::4, Apple##3::3, Apple##2::2),
        Seq (Apple##3::3, Apple##2::2))

    val apple7 = (
        Seq (Apple##4::4, Apple##3::3),
        Seq (Apple##3::3))

    val apples = Seq (apple1, apple2, apple3, apple4, apple5, apple6, apple7)

    val banana1 = (
        Seq (Banana##1::1),
        Seq (Banana##1::1))

    val banana2 = (
        Seq (Banana##2::2),
        Seq (Banana##2::2))

    val banana3 = (
        Seq (Banana##2::2, Banana##1::1),
        Seq (Banana##2::2))

    val banana4 = (
        Seq (Banana##3::3, Banana##2::2, Banana##1::1),
        Seq (Banana##3::3, Banana##2::2))

    val banana5 = (
        Seq (Banana##4::4, Banana##3::3, Banana##2::2, Banana##1::1),
        Seq (Banana##3::3, Banana##2::2))

    val banana6 = (
        Seq (Banana##4::4, Banana##3::3, Banana##2::2),
        Seq (Banana##3::3, Banana##2::2))

    val banana7 = (
        Seq (Banana##4::4, Banana##3::3),
        Seq (Banana##3::3))

    val bananas = Seq (banana1, banana2, banana3, banana4, banana5, banana6, banana7)

    test ((Seq.empty, Seq.empty))
    for (a <- apples)
      test (a)
    for (a <- apples; b <- bananas)
      test (concat (a, b))
  }}
