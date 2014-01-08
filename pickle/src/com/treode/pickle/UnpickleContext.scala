package com.treode.pickle

import java.io.DataInput
import scala.collection.mutable

abstract class UnpickleContext private [pickle] {

  private [this] val m = mutable.Map [Int, Any]()

  private [pickle] def get [A] (idx: Int) = m (idx) .asInstanceOf [A]

  private [pickle] def put [A] (v: A) = m.put (m.size, v)

  def readBytes (data: Array [Byte], offset: Int, length: Int)

  def readByte(): Byte
  def readShort(): Short
  def readInt(): Int
  def readVarInt(): Int
  def readVarUInt(): Int
  def readLong(): Long
  def readVarLong(): Long
  def readVarULong(): Long
  def readFloat(): Float
  def readDouble(): Double
  def readString(): String

  def toDataInput: DataInput
}
