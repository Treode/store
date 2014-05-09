package com.treode.disk.stubs

import com.treode.async.Callback
import com.treode.async.io.stubs.StubFile

class StubDiskDrive {

  private [stubs] var file: StubFile = null

  def stop: Boolean = file.stop

  def stop_= (v: Boolean): Unit = file.stop = v

  def last: Callback [Unit] = file.last
}
