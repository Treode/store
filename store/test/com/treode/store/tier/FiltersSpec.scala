package com.treode.store.tier

import com.treode.async.{AsyncImplicits, StubScheduler}
import com.treode.store.{Bytes, Cell, Fruits}
import org.scalatest.FreeSpec

import AsyncImplicits._
import Fruits.{Apple, Banana}
import TierTestTools._

class FiltersSpec extends FreeSpec {

  def testStringOf (cell: Cell): String = {
    val k = cell.key.string
    cell.value match {
      case Some (v) => s"$k::${v.int}"
      case None => s"$k::_"
    }}

  def testStringOf (cells: Seq [Cell]): String =
    cells.map (testStringOf _) .mkString ("[", ", ", "]")

  "The dedupe filter should" - {

    def test (items: (Seq [Cell], Seq [Cell])*) {
      val in = items .map (_._1) .flatten
      val out = items .map (_._2) .flatten
      s"handle ${testStringOf (in)}" in {
        implicit val scheduler = StubScheduler.random()
        assertResult (out) (Filters.dedupe (in.iterator.async) .toSeq)
      }}

    val apple1 = (
        Seq (Apple##0::1),
        Seq (Apple##0::1))

    val apple2 = (
        Seq (Apple##0::2, Apple##0::1),
        Seq (Apple##0::2))

    val apple3 = (
        Seq (Apple##0::3, Apple##0::2, Apple##0::1),
        Seq (Apple##0::3))

    val banana1 = (
        Seq (Banana##0::1),
        Seq (Banana##0::1))

    val banana2 = (
        Seq (Banana##0::2, Banana##0::1),
        Seq (Banana##0::2))

    val banana3 = (
        Seq (Banana##0::3, Banana##0::2, Banana##0::1),
        Seq (Banana##0::3))

    test ()
    test (apple1)
    test (apple2)
    test (apple3)
    test (apple1, banana1)
    test (apple2, banana1)
    test (apple3, banana1)
    test (apple1, banana2)
    test (apple2, banana2)
    test (apple3, banana2)
    test (apple1, banana3)
    test (apple2, banana3)
    test (apple3, banana3)
  }}
