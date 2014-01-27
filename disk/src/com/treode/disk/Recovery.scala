package com.treode.disk

import com.treode.async.Callback

trait Recovery {

  def reload [B] (desc: RootDescriptor [B]) (f: (B, Callback [Unit]) => Any)

  def replay [R] (desc: RecordDescriptor [R]) (f: R => Any)

  def close (f: => Any)
}
