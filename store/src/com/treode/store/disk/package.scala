package com.treode.store

import com.treode.cluster.concurrent.Callback

package disk {

  private [store] trait Page

  private [store] trait DiskSystem {

    def maxPageSize: Int

    def read (pos: Long, cb: Callback [Page])

    def write (page: Page, cb: Callback [Long])
  }}
