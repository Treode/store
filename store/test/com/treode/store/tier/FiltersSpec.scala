package com.treode.store.tier

import com.treode.async.{AsyncImplicits, StubScheduler}
import com.treode.store.{Bytes, Fruits}
import org.scalatest.FreeSpec

import AsyncImplicits._
import Fruits.{Apple, Banana}
import TierTestTools._

class FiltersSpec extends FreeSpec {

  def testStringOf (cell: TierCell): String = {
    val k = cell.key.string
    cell.value match {
      case Some (v) => s"$k::${v.int}"
      case None => s"$k::_"
    }}

  def testStringOf (cells: Seq [TierCell]): String =
    cells.map (testStringOf _) .mkString ("[", ", ", "]")

  "The dedupe filter should" - {

    def test (items: (Seq [TierCell], Seq [TierCell])*) {
      val in = items .map (_._1) .flatten
      val out = items .map (_._2) .flatten
      s"handle ${testStringOf (in)}" in {
        implicit val scheduler = StubScheduler.random()
        assertResult (out) (Filters.dedupe (in.iterator.async) .toSeq)
      }}

    val apple1 = (
        Seq (Apple::1),
        Seq (Apple::1))

    val apple2 = (
        Seq (Apple::2, Apple::1),
        Seq (Apple::2))

    val apple3 = (
        Seq (Apple::3, Apple::2, Apple::1),
        Seq (Apple::3))

    val banana1 = (
        Seq (Banana::1),
        Seq (Banana::1))

    val banana2 = (
        Seq (Banana::2, Banana::1),
        Seq (Banana::2))

    val banana3 = (
        Seq (Banana::3, Banana::2, Banana::1),
        Seq (Banana::3))

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
