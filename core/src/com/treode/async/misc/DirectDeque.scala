package com.treode.async.misc

import java.lang.System
import scala.reflect.ClassTag

/**
 * DirectDeque is a queue (FIFO ordering) that supports
 * constant time access to any element in the queue
 */
private class DirectDeque [M] (init_capacity: Int = 16) (implicit
   mtag: ClassTag[M]
) extends Traversable[M] {
  private var front = 0
  private var back = front

  private var array = new Array[M](init_capacity)

  override def size = {
    if (front <= back) {
      back-front
    } else {
      (array.length-front)+back
    }
  }

  override def isEmpty = {
    front == back
  }

 /**
  * Adds element to the end of this DirectDeque
  */
  def enqueue (element: M) {
    array(back) = element

    var len = array.length
    var next = (back+1)%len
    if (next == front) {
      array = resize()
      front = 0
      back = len
    } else {
      back = next
    }
  }

  /**
   * Removes element from the front of this DirectDeque 
   * in a first in first out (FIFO) order
   * throws NoSuchElementException if there are no elements
   */
  def dequeue() : M = {
    if (isEmpty) {
      throw new NoSuchElementException()
    }

    var retval = array(front)

    front = (front+1)%array.length

    retval
  }

  /**
   * Returns but does not remove the nth element in
   * this DirectDeque
   * throws NoSuchElementException if n is < 0 or > number of elements
   */
  def get (n: Int) : M = {
    if (n < 0 || n > size-1) {
      throw new NoSuchElementException()
    }

    array((front+n)%array.length)
  }

  def foreach[U](f: M => U) {
    for (i <- 0 to size-1) {
      f(get(i))
    }
  }

  /**
   * Returns a new array of double the size of the original array
   * with the original elements copied into it
   */
  private def resize() : Array[M] = {
    var prevlen = array.length
    var newarray = new Array[M](2*prevlen)
    
    // copy elements from front till end of original array
    System.arraycopy(array, front, newarray, 0, prevlen-front)
    if (back < front) {
       // if back has wrapped around and is before front,
       // copy the front elements from original array to end of newarray
      System.arraycopy(array, 0, newarray, prevlen-front, front)    
    }

    newarray
  }}