package com.treode.async.misc

import org.scalatest.FlatSpec

class DirectDequeSpec extends FlatSpec {
  
  "DirectDeque" should "add one integer to the end of the deque" in {
    val deque = new DirectDeque[Int]
    // enqueue one element
    deque.enqueue(1)
    assert (!deque.isEmpty())
    assert (deque.size == 1)
  }

  it should "be able to add more than the deque initial capacity" in {
    val capacity = 5
    val deque = new DirectDeque[Int](capacity)
    // enqueue more than the initial capacity
    for (i <- 1 to capacity+1) {
      deque.enqueue(i)
    }
    assert (!deque.isEmpty())
    assert (deque.size == capacity+1)
  }
  
  it should "throw NoSuchElementException when get(n) on an empty deque" in {
    val deque = new DirectDeque[Int]

    assert (deque.isEmpty())
    intercept[NoSuchElementException] { deque.get(0) }
  }

  it should "throw NoSuchElementException when get(n) on an invalid n" in {
    val deque = new DirectDeque[Int]

    // enqueue 10 elements
    for (i <- 1 to 10) {
      deque.enqueue(i)
    }
    assert (!deque.isEmpty())
    assert (deque.size == 10)

    intercept[NoSuchElementException] { deque.get(-1) }
    intercept[NoSuchElementException] { deque.get(10) }
    intercept[NoSuchElementException] { deque.get(11) }
  }

  it should "get but not delete the nth integer in deque" in {
    val capacity = 5
    val deque = new DirectDeque[Int](capacity)

    for (i <- 1 to capacity) {
      deque.enqueue(i)
    }
    assert (!deque.isEmpty())
    assert (deque.size == capacity)
    for (i <- 1 to capacity) {
      assert (deque.get(i-1) == i)
      assert (deque.size == capacity)
    }
  }

  it should "throw NoSuchElementException when empty deque is dequeued" in {
    val deque = new DirectDeque[Int]

    assert (deque.isEmpty())
    intercept[NoSuchElementException] { deque.dequeue() }
  }

  it should "enqueue, dequeue, and dequeue, which throws NoSuchElementException" in {
    val deque = new DirectDeque[Int]
    // enqueue one element
    deque.enqueue(1)
    assert (deque.size == 1)
    assert (deque.get(0) == 1)
    // dequeue that one element
    var element = deque.dequeue()
    assert (element == 1)
    assert (deque.isEmpty())

    intercept[NoSuchElementException] { deque.dequeue() }
  }
  
  it should "do 2 enqueues, 2 dequeues, and a dequeue, which throws NoSuchElementException" in {
    val deque = new DirectDeque[Int]
    // enqueue two elements
    deque.enqueue(1)
    assert (deque.size == 1)
    assert (deque.get(0) == 1)

    deque.enqueue(2)
    for (i <- 1 to 2) {
      assert (deque.get(i-1) == i)
      assert (deque.size == 2)
    }

    // dequeue the two elements: should return in the order they were added to the deque
    var element = deque.dequeue()
    assert (element == 1)
    assert (deque.size == 1)
    assert (deque.get(0) == 2)

    element = deque.dequeue()
    assert (element == 2)
    assert (deque.isEmpty())
    intercept[NoSuchElementException] { deque.get(0) }

    intercept[NoSuchElementException] { deque.dequeue() }
  }

  it should "enqueue and dequeue twice, and a dequeue, which throws NoSuchElementException" in {
    println("the test you want")

    val deque = new DirectDeque[Int]

    deque.enqueue(1)
    assert (deque.size == 1)
    assert (deque.get(0) == 1)

    var element = deque.dequeue()
    assert (element == 1)
    assert (deque.isEmpty())
    intercept[NoSuchElementException] { deque.get(0) }

    deque.enqueue(2)
    assert (deque.size == 1)
    assert (deque.get(0) == 2)

    element = deque.dequeue()
    assert (element == 2)
    assert (deque.isEmpty())
    intercept[NoSuchElementException] { deque.get(0) }
    
    intercept[NoSuchElementException] { deque.dequeue() }
  }

  it should "do 3 enqueues, 3 dequeues, 1 enqueue, 1 dequeue, and a dequeue, which throws NoSuchElementException" in {
    val deque = new DirectDeque[Int]
    // enqueue two elements
    deque.enqueue(1)
    assert (deque.size == 1)
    assert (deque.get(0) == 1)

    deque.enqueue(2)
    assert (deque.size == 2)
    for (i <- 1 to 2) {
      assert (deque.get(i-1) == i)
      assert (deque.size == 2)
    }

    deque.enqueue(3)
    assert (deque.size == 3)
    for (i <- 1 to 3) {
      assert (deque.get(i-1) == i)
      assert (deque.size == 3)
    }

    // dequeue the three elements: should return in the order they were added to the deque
    var element = deque.dequeue()
    assert (element == 1)
    assert (deque.size == 2)
    assert (deque.get(0) == 2)
    assert (deque.get(1) == 3)

    element = deque.dequeue()
    assert (element == 2)
    assert (deque.size == 1)
    assert (deque.get(0) == 3)

    element = deque.dequeue()
    assert (element == 3)
    assert (deque.isEmpty())
    intercept[NoSuchElementException] { deque.get(0) }

    deque.enqueue(1)
    assert (deque.size == 1)
    assert (deque.get(0) == 1)

    element = deque.dequeue()
    assert (element == 1)
    assert (deque.isEmpty())
    intercept[NoSuchElementException] { deque.get(0) }

    intercept[NoSuchElementException] { deque.dequeue() }
  }}