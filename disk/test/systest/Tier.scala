package systest

import com.treode.async.Callback
import com.treode.disk.{Disks, Position}
import com.treode.pickle.Picklers

case class Tier (gen: Long, root: Position) {

  def read (key: Int, cb: Callback [Option [Cell]]) (implicit disks: Disks) {

    val epoch = disks.join (cb)

    val loop = new Callback [TierPage] {

      def pass (p: TierPage) {
        p match {
          case p: IndexPage =>
            val i = p.find (key)
            if (i == p.size) {
              epoch (None)
            } else {
              val e = p.get (i)
              TierPage.pager.read (e.pos, this)
            }
          case p: CellPage =>
            val i = p.find (key)
            if (i == p.size) {
              epoch (None)
            } else {
              val e = p.get (i)
              if (e.key == key)
                epoch (Some (e))
              else
                epoch (None)
            }}}

      def fail (t: Throwable) = epoch.fail (t)
    }

    TierPage.pager.read (root, loop)
  }


}

object Tier {

  val pickler = {
    import Picklers._
    val pos = Position.pickler
    wrap (ulong, pos)
    .build ((Tier.apply _).tupled)
    .inspect (v => (v.gen, v.root))
  }}
