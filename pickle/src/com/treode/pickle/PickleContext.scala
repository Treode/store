package com.treode.pickle

import scala.collection.mutable

abstract class PickleContext private [pickle] {

  private [this] val m = mutable.Map [Any, Int]()

  private [pickle] def contains (v: Any) = m contains v

  private [pickle] def get (v: Any) = m (v)

  private [pickle] def put (v: Any) = m.put (v, m.size)

  def writeBytes (data: Array [Byte], offset: Int, length: Int)

  def writeByte (v: Byte)
  def writeShort (v: Short)
  def writeInt (v: Int)
  def writeVarInt (v: Int)
  def writeVarUInt (v: Int)
  def writeLong (v: Long)
  def writeVarLong (v: Long)
  def writeVarULong (v: Long)
  def writeFloat (v: Float)
  def writeDouble (v: Double)
  def writeString (v: String)
}
