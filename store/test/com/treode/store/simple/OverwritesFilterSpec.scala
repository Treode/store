package com.treode.store.simple

import com.treode.async.{AsyncIterator, Callback, CallbackCaptor, Scheduler}
import com.treode.store.{Bytes, Fruits}
import org.scalatest.FlatSpec

import Fruits.{Apple, Banana, Orange}
import SimpleTestTools._

class OverwritesFilterSpec extends FlatSpec {

  implicit val scheduler: Scheduler =
    new Scheduler {
      def execute (task: Runnable): Unit = task.run()
      def delay (millis: Long, task: Runnable): Unit = task.run()
      def at (millis: Long, task: Runnable): Unit = task.run()
      def spawn (task: Runnable): Unit = task.run()
    }

  private def expectCells (cs: SimpleCell*) (actual: AsyncIterator [SimpleCell]) {
    val cb = CallbackCaptor [Seq [SimpleCell]]
    AsyncIterator.scan (actual, cb)
    expectResult (cs) (cb.passed)
  }

  private def newFilter (cs: SimpleCell*) = {
    val cb = CallbackCaptor [AsyncIterator [SimpleCell]]
    OverwritesFilter (AsyncIterator.adapt (cs.iterator), cb)
    cb.passed
  }

  "The OverwritesFilter" should "handle []" in {
    expectCells () (newFilter ())
  }

  it should "handle [Apple::1]" in {
    expectCells (Apple::1) (newFilter (Apple::1))
  }

  it should "handle [Apple::2, Apple::1]" in {
    expectCells (Apple::2) (newFilter (Apple::2, Apple::1))
  }

  it should "handle [Apple::1, Banana::1]" in {
    expectCells (Apple::1, Banana::1) (newFilter (Apple::1, Banana::1))
  }

  it should "handle [Apple::2, Apple::1, Banana::1]" in {
    expectCells (Apple::2, Banana::1) (newFilter (Apple::2, Apple::1, Banana::1))
  }

  it should "handle [Apple::1, Banana::2, Banana::2]" in {
    expectCells (Apple::1, Banana::2) (newFilter (Apple::1, Banana::2, Banana::1))
  }

  it should "handle [Apple::1, Banana::1, Orange::1]" in {
    expectCells (Apple::1, Banana::1, Orange::1) (newFilter (Apple::1, Banana::1, Orange::1))
  }

  it should "handle [Apple::2, Apple::1, Banana::1, Orange::1]" in {
    expectCells (Apple::2, Banana::1, Orange::1) (
        newFilter (Apple::2, Apple::1, Banana::1, Orange::1))
  }

  it should "handle [Apple::1, Banana, Banana::1, Orange::1]" in {
    expectCells (Apple::1, Banana::2, Orange::1) (
        newFilter (Apple::1, Banana::2, Banana::1, Orange::1))
  }

  it should "handle [Apple::1, Banana::1, Orange, Orange::1]" in {
    expectCells (Apple::1, Banana::1, Orange::2) (
        newFilter (Apple::1, Banana::1, Orange::2, Orange::1))
  }}
