package systest

import com.treode.async.Callback
import com.treode.disk.Recovery

trait Table {

  def put (key: Int, value: Int, cb: Callback [Unit])
  def delete (key: Int, cb: Callback [Unit])
  def iterator (cb: Callback [CellIterator])
}

object Table {

  def recover (cb: Callback [Table]) (implicit recovery: Recovery, config: TestConfig): Unit =
    SynthTable.recover (cb)
}
