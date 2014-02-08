package systest

import com.treode.disk.{PageDescriptor, TypeId}
import com.treode.pickle.Picklers

trait TierPage

object TierPage {

  val pickler = {
    import Picklers._
    tagged [TierPage] (
      0x1 -> IndexPage.pickler,
      0x2 -> CellPage.pickler)
  }

  val pager = {
    import Picklers._
    new PageDescriptor (0x3A801319, ulong, pickler)
  }}
