package com.treode.pickle

import java.io.DataInput
import scala.collection.mutable

import com.treode.buffer.Input

abstract class UnpickleContext private [pickle] extends Input {

  private [this] val m = mutable.Map [Int, Any]()

  private [pickle] def get [A] (idx: Int) = m (idx) .asInstanceOf [A]

  private [pickle] def put [A] (v: A) = m.put (m.size, v)

  def toDataInput: DataInput
}
