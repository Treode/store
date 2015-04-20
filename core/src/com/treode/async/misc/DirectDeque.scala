package com.treode.async.misc

import java.lang.System
import scala.reflect.ClassTag

/** DirectDeque is a FIFO queue that supports random access. DirectDeque exists because Java's
  * ArrayDeque lacks `get(i)`.
  */
private class DirectDeque [M] (initialSize: Int = 16) (implicit mtag: ClassTag [M])
extends Traversable [M] {

  private var front = 0
  private var back = 0
  private var array = new Array [M] (initialSize)

  override def size: Int =
    if (front <= back)
      back - front
    else
      array.length - front + back

  override def isEmpty: Boolean =
    front == back

  /** Adds element to the end. */
  def enqueue (element: M) {
    array(back) = element
    var len = array.length
    var next = (back + 1) % len
    if (next == front) {
      array = resize()
      front = 0
      back = len
    } else {
      back = next
    }
  }

  /** Removes element from the front.
    * @throws NoSuchElementException If there are no elements.
    */
  def dequeue(): M = {
    if (isEmpty)
      throw new NoSuchElementException()
    var retval = array(front)
    front = (front+1)%array.length
    retval
  }

  /** Returns the nth element.
    * @throws NoSuchElementException If `n < 0` or `n >= size`
    */
  def get (n: Int): M = {
    if (n < 0 || n > size - 1)
      throw new NoSuchElementException
    array ((front + n) % array.length)
  }

  def foreach [U] (f: M => U) {
    for (i <- 0 to size-1) {
      f(get(i))
    }
  }

  /** Returns a new array of double the size of the original array, with the original elements
    * copied into it.
    */
  private def resize(): Array [M] = {
    var prevlen = array.length
    var newarray = new Array [M] (2 * prevlen)

    // Copy elements from front till end of original array.
    System.arraycopy (array, front, newarray, 0, prevlen - front)
    if (back < front) {
       // If back has wrapped around before front, then the logical back of the deque is at the
       // physical front of the original array. Copy those to the physical back of the new array.
      System.arraycopy (array, 0, newarray, prevlen - front, front)
    }

    newarray
  }
}
