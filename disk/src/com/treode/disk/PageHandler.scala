package com.treode.disk

import com.treode.async.Async

trait PageHandler [G] {

  /** Returns those groups which are still referenced. */
  def probe (groups: Set [G]): Async [Set [G]]

  def compact (groups: Set [G]): Async [Unit]
}
