package com.treode.pickle

import java.io.DataOutput
import scala.collection.mutable

import com.treode.buffer.Output

abstract class PickleContext private [pickle] extends Output {

  private [this] val m = mutable.Map [Any, Int]()

  private [pickle] def contains (v: Any) = m contains v

  private [pickle] def get (v: Any) = m (v)

  private [pickle] def put (v: Any) = m.put (v, m.size)

  def toDataOutput: DataOutput
}
