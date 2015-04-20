package com.treode.async.misc

import org.scalatest.FlatSpec

class DirectDequeSpec extends FlatSpec {
	
	"DirectDeque" should "add one integer to the end of the deque" in {
		val deque = new DirectDeque[Int]
		// enqueue one element
		deque.enqueue(1)
		assert (!deque.isEmpty())
		assert (deque.size() == 1)
	}

	it should "be able to add more than the deque initial capacity" in {
		val deque = new DirectDeque[Int]
		// enqueue 17 elements
		for (i <- 1 to 17) {
			deque.enqueue(i)
		}
		assert (!deque.isEmpty())
		assert (deque.size() == 17)
	}

	it should "get but not delete the first integer in deque" in {
		val deque = new DirectDeque[Int]
		// enqueue one element
		deque.enqueue(1)
		assert (!deque.isEmpty())
		assert (deque.size() == 1)
		var element = deque.get(0)
		assert (element == 1)
		assert (!deque.isEmpty())
		assert (deque.size() == 1)
	}	

	it should "get but not delete the nth integer in deque" in {
		val deque = new DirectDeque[Int]
		// enqueue 17 elements
		for (i <- 1 to 17) {
			deque.enqueue(i)
		}
		assert (!deque.isEmpty())
		assert (deque.size() == 17)
		for (i <- 1 to 17) {
			assert (deque.get(i-1) == i)
		}
		assert (!deque.isEmpty())
		assert (deque.size() == 17)
	}

	it should "get but not delete the nth integer in circular deque" in {
		val deque = new DirectDeque[Int]
		// enqueue 16 elements
		for (i <- 1 to 16) {
			deque.enqueue(i)
		}
		assert (deque.size() == 16)
		for (i <- 1 to 4) {
			deque.dequeue()
		}
		assert (deque.size() == 12)
		for (i <- 17 to 20) {
			deque.enqueue(i)
		}
		assert (deque.size() == 16)
		// test get(n) when it is a circular deque
		for (i <- 5 to 20) {
			assert (deque.get(i-5) == i)
		}
		assert (!deque.isEmpty())
		assert (deque.size() == 16)
	}

	it should "throw NoSuchElementException when get(n) on an invalid n" in {
		val deque = new DirectDeque[Int]

		// enqueue 10 elements
		for (i <- 1 to 10) {
			deque.enqueue(i)
		}
		assert (!deque.isEmpty())
		assert (deque.size() == 10)

		intercept[NoSuchElementException] { deque.get(-1) }
		intercept[NoSuchElementException] { deque.get(10) }
		intercept[NoSuchElementException] { deque.get(11) }
	}

	it should "delete and return the one integer in the deque" in {
		val deque = new DirectDeque[Int]
		// enqueue one element
		deque.enqueue(1)
		assert (!deque.isEmpty())
		assert (deque.size() == 1)
		// dequeue that one element
		var element = deque.dequeue()
		assert (element == 1)
		assert (deque.isEmpty())
		assert (deque.size() == 0)
	}
	
	it should "delete and return integers in deque in FIFO order" in {
		val deque = new DirectDeque[Int]
		// enqueue two elements
		deque.enqueue(1)
		deque.enqueue(2)
		assert (!deque.isEmpty())
		assert (deque.size() == 2)
		// dequeue the two elements in the order they were added to the deque
		var element = deque.dequeue()
		assert (element == 1)
		assert (deque.size() == 1)
		element = deque.dequeue()
		assert (element == 2)
		assert (deque.isEmpty())
		assert (deque.size() == 0)
	}

	it should "throw NoSuchElementException when empty deque is dequeued" in {
		val deque = new DirectDeque[Int]

		assert (deque.isEmpty())
		intercept[NoSuchElementException] { deque.dequeue() }
	}}