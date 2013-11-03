package com.treode.store.tier

import com.treode.pickle.{Buffer, Picklers, pickle, unpickle}
import com.treode.store.{Bytes, Fruits, TxClock}
import org.scalatest.WordSpec

class CellPageSpec extends WordSpec {
  import Fruits.{Apple, Kiwi, Orange}

  val MaxTime = TxClock.MaxValue

  private def entry (key: Bytes, time: Int): Cell =
    new Cell (key, time, None)

  private def newPage (entries: Cell*): CellPage =
    new CellPage (Array (entries: _*))

  private def entriesEqual (expected: Cell, actual: Cell) {
    expectResult (expected.key) (actual.key)
    expectResult (expected.time) (actual.time)
    expectResult (expected.value) (actual.value)
  }

  private def pagesEqual (expected: CellPage, actual: CellPage) {
    expectResult (expected.entries.length) (actual.entries.length)
    for (i <- 0 until expected.entries.length)
      entriesEqual (expected.entries (i), actual.entries (i))
  }

  private def checkPickle (page: CellPage) {
    val buffer = Buffer (12)
    pickle (CellPage.pickle, page, buffer)
    val result = unpickle (CellPage.pickle, buffer)
    pagesEqual (page, result)
  }

  "A CellPage" when {

    "empty" should {

      val page = newPage ()

      "find nothing" in {
        expectResult (0) (page.find (Apple, MaxTime))
      }

      "pickle and unpickle to the same value" in {
        checkPickle (page)
      }}

    "holding one entry k=kiwi at t=7" should {

      val page = newPage (entry (Kiwi, 7))

      "find apple before kiwi" in {
        expectResult (0) (page.find (Apple, MaxTime))
      }

      "find kiwi using kiwi at t=8" in {
        expectResult (0) (page.find (Kiwi, 8))
      }

      "find kiwi using kiwi at t=7" in {
        expectResult (0) (page.find (Kiwi, 7))
      }

      "find orange using kiwi at t=6" in {
        expectResult (1) (page.find (Kiwi, 6))
      }

      "find orange after kiwi" in {
        expectResult (1) (page.find (Orange, MaxTime))
      }

      "pickle and unpickle to the same value" in {
        checkPickle (page)
      }}

    "holding three keys at t=7" should {

      val page = newPage (
          entry (Apple, 7),
          entry (Kiwi, 7),
          entry (Orange, 7))

      "find apple using (apple, 8)" in {
        expectResult (0) (page.find (Apple, 8))
      }

      "find apple using (apple, 7)" in {
        expectResult (0) (page.find (Apple, 7))
      }

      "find kiwi using (apple, 6)" in {
        expectResult (1) (page.find (Apple, 6))
      }

      "find kiwi using (kiwi, 8)" in {
        expectResult (1) (page.find (Kiwi, 8))
      }

      "find kiwi using (kiwi, 7)" in {
        expectResult (1) (page.find (Kiwi, 7))
      }

      "find orange using (kiwi, 6)" in {
        expectResult (2) (page.find (Kiwi, 6))
      }

      "find orange using (orange, 8)" in {
        expectResult (2) (page.find (Orange, 8))
      }

      "find orange using (orange, 7)" in {
        expectResult (2) (page.find (Orange, 7))
      }

      "find the end using (orange, 6)" in {
        expectResult (3) (page.find (Orange, 6))
      }

      "pickle and unpickle to the same value" in {
        checkPickle (page)
      }}

    "holding three entries of kiwi" should {

      val page = newPage (
          entry (Kiwi, 21),
          entry (Kiwi, 14),
          entry (Kiwi, 7))

      "find (kiwi, 21) using (kiwi, 22)" in {
        expectResult (0) (page.find (Kiwi, 22))
      }

      "find (kiwi, 21) using (kiwi, 21)" in {
        expectResult (0) (page.find (Kiwi, 21))
      }

      "find (kiwi, 14) using (kiwi, 20)" in {
        expectResult (1) (page.find (Kiwi, 20))
      }

      "find (kiwi, 14) using (kiwi, 15)" in {
        expectResult (1) (page.find (Kiwi, 15))
      }

      "find (kiwi, 14) using (kiwi, 14)" in {
        expectResult (1) (page.find (Kiwi, 14))
      }

      "find (kiwi, 7) using (kiwi, 13)" in {
        expectResult (2) (page.find (Kiwi, 13))
      }

      "find (kiwi, 7) using (kiwi, 8)" in {
        expectResult (2) (page.find (Kiwi, 8))
      }

      "find (kiwi, 7) using (kiwi, 7)" in {
        expectResult (2) (page.find (Kiwi, 7))
      }

      "find the end using (kiwi, 6)" in {
        expectResult (3) (page.find (Kiwi, 6))
      }

      "pickle and unpickle to the same value" in {
        checkPickle (page)
      }}}}
