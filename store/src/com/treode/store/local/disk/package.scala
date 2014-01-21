package com.treode.store.local

import com.treode.async.Callback
import com.treode.disk.Position

package disk {

  private [store] trait Page

  private [store] trait DiskSystem {

    def maxPageSize: Int

    def read (pos: Position, cb: Callback [Page])

    def write (page: Page, cb: Callback [Position])
  }}
