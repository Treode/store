/*
 * Copyright 2014 Treode, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.treode.disk

import scala.collection.JavaConversions._
import com.googlecode.javaewah.{EWAHCompressedBitmap => Bitmap}
import com.treode.pickle.{Pickler, PickleContext, UnpickleContext}

private class IntSet private (private val bitmap: Bitmap) extends Iterable [Int] {

  def this() = this (Bitmap.bitmapOf())

  def add (i: Int): IntSet =
    new IntSet (bitmap.or (Bitmap.bitmapOf (i)))

  def add (s: IntSet): IntSet =
    new IntSet (bitmap.or (s.bitmap))

  def remove (i: Int): IntSet =
    new IntSet (bitmap.andNot (Bitmap.bitmapOf (i)))

  def remove (s: IntSet): IntSet =
    new IntSet (bitmap.andNot (s.bitmap))

  def complement: IntSet = {
    val dup = bitmap.clone()
    dup.not
    new IntSet (dup)
  }

  def contains (i: Int): Boolean =
    bitmap.get (i)

  def intersects (other: IntSet): Boolean =
    bitmap.intersects (other.bitmap)

  def min: Int =
    bitmap.iterator.next

  def iterator: Iterator [Int] =
    asScalaIterator (bitmap.iterator.map (_.toInt))

  override def size: Int =
    bitmap.cardinality()

  override def hashCode: Int = bitmap.hashCode

  override def equals (other: Any): Boolean =
    other match {
      case that: IntSet => bitmap.equals (that.bitmap)
      case _ => false
    }

  override def clone(): IntSet =
    new IntSet (bitmap.clone())

  override def toString: String =
    s"IntSet (size=${bitmap.cardinality}, byteSize=${bitmap.sizeInBytes})"
}

private object IntSet {

  def apply (is: Int*): IntSet =
    new IntSet (Bitmap.bitmapOf (is: _*))

  def fill (n: Int): IntSet = {
    val bitmap = Bitmap.bitmapOf()
    bitmap.setSizeInBits (n, true)
    new IntSet (bitmap)
  }

  val pickler: Pickler [IntSet] =
    new Pickler [IntSet] {
      def p (v: IntSet, ctx: PickleContext) {
        v.bitmap.serialize (ctx.toDataOutput)
      }
      def u (ctx: UnpickleContext): IntSet = {
        val bitmap = Bitmap.bitmapOf()
        bitmap.deserialize (ctx.toDataInput)
        new IntSet (bitmap)
      }}
}
