package com.treode.async.misc

import org.scalatest.FlatSpec

class DirectDequeSpec extends FlatSpec {

  def assertElements (es: Int*) (deque: DirectDeque[Int]) {
    if (es.isEmpty) {
      assert (deque.isEmpty)

      intercept[NoSuchElementException] { deque.get(0) }
      intercept[NoSuchElementException] { deque.dequeue() }
    } else {
      assert (deque.size == es.size)
      assert (Seq.tabulate (deque.size) (deque.get (_)) == es)

      intercept[NoSuchElementException] { deque.get(-1) }
      intercept[NoSuchElementException] { deque.get(deque.size) }
    }
  }

  "DirectDeque" should "nq more than the deque initial capacity" in {
    val capacity = 3
    val deque = new DirectDeque[Int](capacity)
    // enqueue more than the initial capacity
    for (i <- 1 to capacity+1) {
      deque.enqueue(i)
    }
    assertElements (1, 2, 3, 4) (deque)
  }

  it should "err, nq, dq, err" in {
    val deque = new DirectDeque[Int](1)
    assertElements () (deque)
    // enqueue one element
    deque.enqueue(1)
    assertElements (1) (deque)
    // dequeue that one element
    var element = deque.dequeue()
    assert (element == 1)
    assertElements () (deque)
  }
  
  it should "nq, nq, dq, dq, err" in {
    val deque = new DirectDeque[Int](2)
    // enqueue two elements
    deque.enqueue(1)
    assertElements (1) (deque)

    deque.enqueue(2)
    assertElements (1, 2) (deque)

    // dequeue the two elements
    //  - should return in the order they were added to the deque
    var element = deque.dequeue()
    assert (element == 1)
    assertElements (2) (deque)

    element = deque.dequeue()
    assert (element == 2)
    assertElements () (deque)
  }

  it should "nq, dq, err, nq, dq, err" in {
    val deque = new DirectDeque[Int](2)

    deque.enqueue(1)
    assertElements (1) (deque)

    var element = deque.dequeue()
    assert (element == 1)
    assertElements () (deque)

    deque.enqueue(2)
    assertElements (2) (deque)

    element = deque.dequeue()
    assert (element == 2)
    assertElements () (deque)
  }

  it should "nq, dq, err, nq, nq, dq, dq, err" in {
    val deque = new DirectDeque[Int](2)

    deque.enqueue(1)
    assertElements (1) (deque)

    var element = deque.dequeue()
    assert (element == 1)
    assertElements () (deque)

    deque.enqueue(1)
    assertElements (1) (deque)

    deque.enqueue(2)
    assertElements (1, 2) (deque)  

    element = deque.dequeue()
    assert (element == 1)
    assertElements (2) (deque)

    element = deque.dequeue()
    assert (element == 2)
    assertElements () (deque)
  }

  it should "nq, nq, nq, dq, dq, dq, err, nq, dq, err" in {
    val deque = new DirectDeque[Int](3)
    // enqueue two elements
    deque.enqueue(1)
    assertElements (1) (deque)

    deque.enqueue(2)
    assertElements (1, 2) (deque)

    deque.enqueue(3)
    assertElements (1, 2, 3) (deque)

    // dequeue the three elements
    //  - should return in the order they were added to the deque
    var element = deque.dequeue()
    assert (element == 1)
    assertElements (2, 3) (deque)

    element = deque.dequeue()
    assert (element == 2)
    assertElements (3) (deque)

    element = deque.dequeue()
    assert (element == 3)
    assertElements () (deque)

    deque.enqueue(1)
    assertElements (1) (deque)

    element = deque.dequeue()
    assert (element == 1)
    assertElements () (deque)
  }}