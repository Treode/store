package com.treode.disk

import com.treode.async.Callback

trait PageHandler [G] {

  /** Returns those groups which are still referenced. */
  def probe (groups: Set [G]): Set [G]

  def compact (groups: Set [G], cb: Callback [Unit])
}
