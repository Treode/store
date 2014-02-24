package com.treode.store

import com.treode.async.AsyncIterator

package object atomic {

  private [atomic] type TimedIterator = AsyncIterator [TimedCell]

}
