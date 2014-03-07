package com.treode.disk

import com.treode.async.Async

trait PageHandler [G] {

  /** Returns those groups which are still referenced. */
  def probe (obj: ObjectId, groups: Set [G]): Async [Set [G]]

  def compact (obj: ObjectId, groups: Set [G]): Async [Unit]
}
