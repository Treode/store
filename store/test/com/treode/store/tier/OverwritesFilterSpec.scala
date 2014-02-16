package com.treode.store.tier

import com.treode.async.{AsyncIterator, Callback, CallbackCaptor, Scheduler}
import com.treode.store.{Bytes, Fruits}
import org.scalatest.FlatSpec

import Fruits.{Apple, Banana, Orange}
import TierTestTools._

class OverwritesFilterSpec extends FlatSpec {

  implicit val scheduler: Scheduler =
    new Scheduler {
      def execute (task: Runnable): Unit = task.run()
      def delay (millis: Long, task: Runnable): Unit = task.run()
      def at (millis: Long, task: Runnable): Unit = task.run()
      def spawn (task: Runnable): Unit = task.run()
    }

  private def expectCells (cs: Cell*) (actual: AsyncIterator [Cell]) {
    val cb = CallbackCaptor [Seq [Cell]]
    AsyncIterator.scan (actual, cb)
    expectResult (cs) (cb.passed)
  }

  private def newFilter (cs: Cell*) = {
    val cb = CallbackCaptor [AsyncIterator [Cell]]
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
