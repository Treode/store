package com.treode.store.disk2

import scala.collection.JavaConversions._
import com.googlecode.javaewah.{EWAHCompressedBitmap => Bitmap}
import com.treode.pickle.{Pickler, PickleContext, UnpickleContext}

private class IntSet private (private val bitmap: Bitmap) {

  def this() = this (Bitmap.bitmapOf())

  def add (i: Int): IntSet =
    new IntSet (bitmap.or (Bitmap.bitmapOf (i)))

  def add (s: IntSet): IntSet =
    new IntSet (bitmap.or (s.bitmap))

  def remove (i: Int): IntSet =
    new IntSet (bitmap.andNot (Bitmap.bitmapOf (i)))

  def remove (s: IntSet): IntSet =
    new IntSet (bitmap.andNot (s.bitmap))

  def contains (i: Int): Boolean =
    bitmap.get (i)

  def min: Option [Int] = {
    val it = bitmap.iterator
    if (!it.hasNext) return None
    val i = it.next()
    return Some (i)
  }

  def isEmpty: Boolean =
    !bitmap.iterator.hasNext

  def toSet: Set [Int] =
    bitmap.iterator .map (_.toInt) .toSet [Int]

  override def hashCode: Int = bitmap.hashCode

  override def equals (other: Any): Boolean =
    other match {
      case that: IntSet => bitmap.equals (that.bitmap)
      case _ => false
    }

  override def toString: String =
    s"IntSet (size=${bitmap.cardinality}, byteSize=${bitmap.sizeInBytes})"
}

private object IntSet {

  val MaxValue: Int = Int.MaxValue - Bitmap.wordinbits

  def fill (n: Int): IntSet = {
    val bitmap = Bitmap.bitmapOf()
    bitmap.setSizeInBits (n, true)
    new IntSet (bitmap)
  }

  val pickle: Pickler [IntSet] =
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
