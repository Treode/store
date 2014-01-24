package com.treode.store

import com.treode.async.AsyncIterator

package object simple {

  private [simple] type SimpleIterator = AsyncIterator [SimpleCell]
}
