package com.treode.async.io

import java.util.concurrent.Executor

object ExecutorMock extends Executor {

  def execute (task: Runnable) = task.run()
}
