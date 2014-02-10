package com.treode.store.timed

import com.treode.async.{AsyncIterator, Callback, CallbackCaptor, Scheduler}
import com.treode.store.{Bytes, Cardinals, Fruits, TimedCell, TimedTestTools}
import org.scalatest.FlatSpec

import Cardinals.One
import Fruits.{Apple, Banana}
import TimedTestTools._

class DeletesFilterSpec extends FlatSpec {

  implicit val scheduler: Scheduler =
    new Scheduler {
      def execute (task: Runnable): Unit = task.run()
      def delay (millis: Long, task: Runnable): Unit = task.run()
      def at (millis: Long, task: Runnable): Unit = task.run()
      def spawn (task: Runnable): Unit = task.run()
    }

  private def expectCells (cs: TimedCell*) (actual: AsyncIterator [TimedCell]) {
    val cb = CallbackCaptor [Seq [TimedCell]]
    AsyncIterator.scan (actual, cb)
    expectResult (cs) (cb.passed)
  }

  private def newFilter (cs: TimedCell*) = {
    val cb = CallbackCaptor [AsyncIterator [TimedCell]]
    DeletesFilter (AsyncIterator.adapt (cs.iterator), cb)
    cb.passed
  }

  "The DeletesFilter" should "handle []" in {
    expectCells () (newFilter ())
  }

  it should "handle [Apple##2]" in {
    expectCells () (newFilter (Apple##2))
  }

  it should "handle [Apple##2::One]" in {
    expectCells (Apple##2::One) (newFilter (Apple##2::One))
  }

  it should "handle [Apple##2::One, Apple##1]" in {
    expectCells (Apple##2::One) (newFilter (Apple##2::One, Apple##1))
  }

  it should "handle [Apple##2, Apple##1::One]" in {
    expectCells (Apple##2, Apple##1::One) (newFilter (Apple##2, Apple##1::One))
  }

  it should "handle [Apple##2::One, Apple##1::One]" in {
    expectCells (Apple##2::One, Apple##1::One) (newFilter (Apple##2::One, Apple##1::One))
  }

  it should "handle [Apple##2, Banana##2]" in {
    expectCells () (newFilter (Apple##2, Banana##2))
  }

  it should "handle [Apple##2::One, Banana##2]" in {
    expectCells (Apple##2::One) (newFilter (Apple##2::One, Banana##2))
  }

  it should "handle [Apple##2::One, Apple##1, Banana##2]" in {
    expectCells (Apple##2::One) (newFilter (Apple##2::One, Apple##1, Banana##2))
  }

  it should "handle [Apple##2, Apple##1::One, Banana##2]" in {
    expectCells (Apple##2, Apple##1::One) (newFilter (Apple##2, Apple##1::One, Banana##2))
  }

  it should "handle [Apple##2::One, Apple##1::One, Banana##2]" in {
    expectCells (Apple##2::One, Apple##1::One) (
        newFilter (Apple##2::One, Apple##1::One, Banana##2))
  }

  it should "handle [Apple##2, Banana##2::One]" in {
    expectCells (Banana##2::One) (newFilter (Apple##2, Banana##2::One))
  }

  it should "handle [Apple##2::One, Banana##2::One]" in {
    expectCells (Apple##2::One, Banana##2::One) (newFilter (Apple##2::One, Banana##2::One))
  }

  it should "handle [Apple##2::One, Apple##1, Banana##2::One]" in {
    expectCells (Apple##2::One, Banana##2::One) (
        newFilter (Apple##2::One, Apple##1, Banana##2::One))
  }

  it should "handle [Apple##2, Apple##1::One, Banana##2::One]" in {
    expectCells (Apple##2, Apple##1::One, Banana##2::One) (
        newFilter (Apple##2, Apple##1::One, Banana##2::One))
  }

  it should "handle [Apple##2::One, Apple##1::One, Banana##2::One]" in {
    expectCells (Apple##2::One, Apple##1::One, Banana##2::One) (
        newFilter (Apple##2::One, Apple##1::One, Banana##2::One))
  }

  it should "handle [Apple##2, Banana##2::One, Banana##1]" in {
    expectCells (Banana##2::One) (newFilter (Apple##2, Banana##2::One, Banana##1))
  }

  it should "handle [Apple##2::One, Banana##2::One, Banana##1]" in {
    expectCells (Apple##2::One, Banana##2::One) (
        newFilter (Apple##2::One, Banana##2::One, Banana##1))
  }

  it should "handle [Apple##2::One, Apple##1, Banana##2::One, Banana##1]" in {
    expectCells (Apple##2::One, Banana##2::One) (
        newFilter (Apple##2::One, Apple##1, Banana##2::One, Banana##1))
  }

  it should "handle [Apple##2, Apple##1::One, Banana##2::One, Banana##1]" in {
    expectCells (Apple##2, Apple##1::One, Banana##2::One) (
        newFilter (Apple##2, Apple##1::One, Banana##2::One, Banana##1))
  }

  it should "handle [Apple##2::One, Apple##1::One, Banana##2::One, Banana##1]" in {
    expectCells (Apple##2::One, Apple##1::One, Banana##2::One) (
        newFilter (Apple##2::One, Apple##1::One, Banana##2::One, Banana##1))
  }}
