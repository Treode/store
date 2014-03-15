package example1

import com.twitter.finatra.FinatraServer

class AsyncFinatraServer extends FinatraServer {

  def register (c: AsyncFinatraController): Unit =
    register (c.delegate)
}
