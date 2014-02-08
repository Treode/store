package systest

import scala.collection.JavaConversions._
import com.treode.disk.{Disks, Position}
import com.treode.pickle.Picklers

class Tiers (val tiers: Array [Tier]) {

  def apply (i: Int): Tier = tiers (i)

  def gen: Long =
    if (tiers.size == 0) 0L else tiers (0) .gen

  def active = tiers .map (_.gen) .toSet

  def size = tiers.length
}

object Tiers {

  val empty: Tiers = new Tiers (Array())

  def apply (tier: Tier): Tiers =
    new Tiers (Array (tier))

  val pickler = {
    import Picklers._
    val tier = Tier.pickler
    wrap (array (tier)) .build (new Tiers (_)) .inspect (_.tiers)
  }}
