package com.treode.async.misc

import java.lang.System
import scala.reflect.ClassTag

/**
 * DirectDeque is a queue (FIFO ordering) that supports
 * constant time access to any element in the queue
 */
private class DirectDeque [M] (init_capacity: Int = 16) (implicit
   mtag: ClassTag[M]
) {
  private var front = 0
  private var back = front
  private var empty = true

  private var array = new Array[M](init_capacity)

  def size = {
    if (empty) {
      0
    } else if (front <= back) {
      (back-front)+1
    } else {
      (array.length-front)+back+1
    }
  }

  def isEmpty() : Boolean = {
    size == 0
  }

 /**
  * Adds element to the end of this DirectDeque
  */
  def enqueue (element: M) {
    if ((back+1)%array.length == front) {
      array = resize()
    } else if (!isEmpty()) {
      back = (back+1)%array.length
    }

    array(back) = element
    empty = false
  }

  /**
   * Removes element from the front of this DirectDeque 
   * in a first in first out (FIFO) order
   * throws NoSuchElementException if there are no elements
   */
  def dequeue() : M = {
    if (isEmpty()) {
      throw new NoSuchElementException()
    }

    var retval = array(front)
    if (front != back) {
      front = (front+1)%array.length 
    } else {
      empty = true
    }

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

    front = 0
    back = prevlen

    newarray
  }}