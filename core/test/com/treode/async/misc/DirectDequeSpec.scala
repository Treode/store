package com.treode.async.misc

import org.scalatest.FlatSpec

class DirectDequeSpec extends FlatSpec {
  
  "DirectDeque" should "nq to the end of the deque" in {
    val deque = new DirectDeque[Int]
    // enqueue one element
    deque.enqueue(1)
    assert (!deque.isEmpty())
    assert (deque.size == 1)
    assert (deque.toSeq == Seq(1))
  }

  it should "be able to nq more than the deque initial capacity" in {
    val capacity = 3
    val deque = new DirectDeque[Int](capacity)
    // enqueue more than the initial capacity
    for (i <- 1 to capacity+1) {
      deque.enqueue(i)
    }
    assert (!deque.isEmpty())
    assert (deque.size == capacity+1)
    assert (deque.toSeq == Seq(1, 2, 3, 4))
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
    val capacity = 3
    val deque = new DirectDeque[Int](capacity)

    for (i <- 1 to capacity) {
      deque.enqueue(i)
    }
    assert (!deque.isEmpty())
    assert (deque.size == capacity)
    assert (Seq.tabulate (deque.size) (deque.get (_)) == Seq (1, 2, 3))
    assert (deque.size == capacity)
  }

  it should "throw NoSuchElementException when empty deque is dequeued" in {
    val deque = new DirectDeque[Int]

    assert (deque.isEmpty())
    intercept[NoSuchElementException] { deque.dequeue() }
  }

  it should "nq, dq, err" in {
    val deque = new DirectDeque[Int](1)
    // enqueue one element
    deque.enqueue(1)
    assert (deque.size == 1)
    assert (Seq.tabulate (deque.size) (deque.get (_)) == Seq (1))
    // dequeue that one element
    var element = deque.dequeue()
    assert (element == 1)
    assert (deque.isEmpty())

    intercept[NoSuchElementException] { deque.dequeue() }
  }
  
  it should "nq, nq, dq, dq, err" in {
    val deque = new DirectDeque[Int](2)
    // enqueue two elements
    deque.enqueue(1)
    assert (deque.size == 1)
    assert (Seq.tabulate (deque.size) (deque.get (_)) == Seq (1))

    deque.enqueue(2)
    assert (deque.size == 2)
    assert (Seq.tabulate (deque.size) (deque.get (_)) == Seq (1, 2))

    // dequeue the two elements: should return in the order they were added to the deque
    var element = deque.dequeue()
    assert (element == 1)
    assert (deque.size == 1)
    assert (Seq.tabulate (deque.size) (deque.get (_)) == Seq (2))

    element = deque.dequeue()
    assert (element == 2)
    assert (deque.isEmpty())
    intercept[NoSuchElementException] { deque.get(0) }

    intercept[NoSuchElementException] { deque.dequeue() }
  }

  it should "nq, dq, err, nq, dq, err" in {
    val deque = new DirectDeque[Int](2)

    deque.enqueue(1)
    assert (deque.size == 1)
    assert (Seq.tabulate (deque.size) (deque.get (_)) == Seq (1))

    var element = deque.dequeue()
    assert (element == 1)
    assert (deque.isEmpty())
    intercept[NoSuchElementException] { deque.get(0) }

    intercept[NoSuchElementException] { deque.dequeue() }

    deque.enqueue(2)
    assert (deque.size == 1)
    assert (Seq.tabulate (deque.size) (deque.get (_)) == Seq (2))

    element = deque.dequeue()
    assert (element == 2)
    assert (deque.isEmpty())
    intercept[NoSuchElementException] { deque.get(0) }
    
    intercept[NoSuchElementException] { deque.dequeue() }
  }

  it should "nq, dq, err, nq, nq, dq, dq, err" in {
    val deque = new DirectDeque[Int](2)

    deque.enqueue(1)
    assert (deque.size == 1)
    assert (Seq.tabulate (deque.size) (deque.get (_)) == Seq (1))

    var element = deque.dequeue()
    assert (element == 1)
    assert (deque.isEmpty())
    intercept[NoSuchElementException] { deque.get(0) }

    intercept[NoSuchElementException] { deque.dequeue() }

    deque.enqueue(1)
    assert (deque.size == 1)
    assert (Seq.tabulate (deque.size) (deque.get (_)) == Seq (1))

    deque.enqueue(2)
    assert (deque.size == 2)
    assert (Seq.tabulate (deque.size) (deque.get (_)) == Seq (1, 2))   

    element = deque.dequeue()
    assert (element == 1)
    assert (deque.size == 1)
    assert (Seq.tabulate (deque.size) (deque.get (_)) == Seq (2))

    element = deque.dequeue()
    assert (element == 2)
    assert (deque.isEmpty())
    intercept[NoSuchElementException] { deque.get(0) }
    
    intercept[NoSuchElementException] { deque.dequeue() }
  }

  it should "nq, nq, nq, dq, dq, dq, err, nq, dq, err" in {
    val deque = new DirectDeque[Int](3)
    // enqueue two elements
    deque.enqueue(1)
    assert (deque.size == 1)
    assert (Seq.tabulate (deque.size) (deque.get (_)) == Seq (1))

    deque.enqueue(2)
    assert (deque.size == 2)
    assert (Seq.tabulate (deque.size) (deque.get (_)) == Seq (1, 2))

    deque.enqueue(3)
    assert (deque.size == 3)
    assert (Seq.tabulate (deque.size) (deque.get (_)) == Seq (1, 2, 3))

    // dequeue the three elements: should return in the order they were added to the deque
    var element = deque.dequeue()
    assert (element == 1)
    assert (deque.size == 2)
    assert (Seq.tabulate (deque.size) (deque.get (_)) == Seq (2, 3))

    element = deque.dequeue()
    assert (element == 2)
    assert (deque.size == 1)
    assert (Seq.tabulate (deque.size) (deque.get (_)) == Seq (3))

    element = deque.dequeue()
    assert (element == 3)
    assert (deque.isEmpty())
    intercept[NoSuchElementException] { deque.get(0) }

    intercept[NoSuchElementException] { deque.dequeue() }

    deque.enqueue(1)
    assert (deque.size == 1)
    assert (Seq.tabulate (deque.size) (deque.get (_)) == Seq (1))

    element = deque.dequeue()
    assert (element == 1)
    assert (deque.isEmpty())
    intercept[NoSuchElementException] { deque.get(0) }

    intercept[NoSuchElementException] { deque.dequeue() }
  }}