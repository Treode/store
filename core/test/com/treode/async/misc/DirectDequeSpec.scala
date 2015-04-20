package com.treode.async.misc

import org.scalatest.FlatSpec

class DirectDequeSpec extends FlatSpec {

  def assertElements (es: Int*) (deque: DirectDeque [Int]) {
    if (es.isEmpty) {
      assert (deque.isEmpty)

      intercept[NoSuchElementException] (deque.get (0))
      intercept[NoSuchElementException] (deque.dequeue())
    } else {
      assert (deque.size == es.size)
      assert (Seq.tabulate (deque.size) (deque.get (_)) == es)

      intercept[NoSuchElementException] (deque.get (-1))
      intercept[NoSuchElementException] (deque.get(deque.size))
    }
  }

  "DirectDeque" should "nq more than the deque initial capacity" in {
    val capacity = 3
    val deque = new DirectDeque[Int](capacity)

    for (i <- 1 to capacity+1) {
      deque.enqueue(i)
    }
    assertElements (1, 2, 3, 4) (deque)
  }

  it should "err, nq, dq, err" in {
    val deque = new DirectDeque[Int](1)
    assertElements () (deque)

    deque.enqueue(1)
    assertElements (1) (deque)

    assert (deque.dequeue() == 1)
    assertElements () (deque)
  }
  
  it should "nq, nq, dq, dq, err" in {
    val deque = new DirectDeque[Int](2)

    deque.enqueue(1)
    assertElements (1) (deque)

    deque.enqueue(2)
    assertElements (1, 2) (deque)

    assert (deque.dequeue() == 1)
    assertElements (2) (deque)

    assert (deque.dequeue() == 2)
    assertElements () (deque)
  }

  it should "nq, dq, err, nq, dq, err" in {
    val deque = new DirectDeque[Int](2)

    deque.enqueue(1)
    assertElements (1) (deque)

    assert (deque.dequeue() == 1)
    assertElements () (deque)

    deque.enqueue(2)
    assertElements (2) (deque)

    assert (deque.dequeue() == 2)
    assertElements () (deque)
  }

  it should "nq, dq, err, nq, nq, dq, dq, err" in {
    val deque = new DirectDeque[Int](2)

    deque.enqueue(1)
    assertElements (1) (deque)

    assert (deque.dequeue() == 1)
    assertElements () (deque)

    deque.enqueue(1)
    assertElements (1) (deque)

    deque.enqueue(2)
    assertElements (1, 2) (deque)  

    assert (deque.dequeue() == 1)
    assertElements (2) (deque)

    assert (deque.dequeue() == 2)
    assertElements () (deque)
  }

  it should "nq, nq, nq, dq, dq, dq, err, nq, dq, err" in {
    val deque = new DirectDeque[Int](3)

    deque.enqueue(1)
    assertElements (1) (deque)

    deque.enqueue(2)
    assertElements (1, 2) (deque)

    deque.enqueue(3)
    assertElements (1, 2, 3) (deque)

    assert (deque.dequeue() == 1)
    assertElements (2, 3) (deque)

    assert (deque.dequeue() == 2)
    assertElements (3) (deque)

    assert (deque.dequeue() == 3)
    assertElements () (deque)

    deque.enqueue(1)
    assertElements (1) (deque)

    assert (deque.dequeue() == 1)
    assertElements () (deque)
  }}