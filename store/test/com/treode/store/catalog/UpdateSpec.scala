package com.treode.store.catalog

import com.treode.pickle.{Pickler, Picklers}
import com.treode.store.Bytes
import org.scalatest.FreeSpec

class UpdateSpec extends FreeSpec {

  val values = Seq (
      0x292C28335A06E344L, 0xB58E76CED969A4C7L, 0xDF20D7F2B8C33B9EL, 0x63D5DAAF0C58D041L,
      0x834727637190788AL, 0x2AE35ADAA804CE32L, 0xE3AA9CFF24BC92DAL, 0xCE33BD811236E7ADL,
      0x7FAF87891BE9818AL, 0x3C15A9283BDFBA51L, 0xE8E45A575513FA90L, 0xE224EF2739907F79L,
      0xFC275E6C532CB3CBL, 0x40C505971288B2DDL, 0xCD1C2FD6707573E1L, 0x2D62491B453DA6A3L,
      0xA079188A7E0C8C39L, 0x46A5B2A69D90533AL, 0xD68C9A2FDAEE951BL, 0x7C781E5CF39A5EB1L)

  "Computing and applying a patch should" - {
    import Picklers._

    def checkDiff [A] (p: Pickler [A], v1: A, v2: A) {
      val b1 = Bytes (p, v1)
      val b2 = Bytes (p, v2)
      val patch = Patch.diff (b1, b2)
      val bp = Patch.patch (b1, patch)
      expectResult (b2) (bp)
      val vp = bp.unpickle (p)
      expectResult (v2) (vp)
    }

    "handle equal longs" in {
      checkDiff (fixedLong, 0x4423014FC535F3AFL, 0x4423014FC535F3AFL)
    }

    "handle consecutive longs" in {
      checkDiff (fixedLong, 0x4423014FC535F3AFL, 0x4423014FC535F3B0L)
    }

    "handle different longs" in {
      checkDiff (fixedLong, 0x4423014FC535F3AFL, 0x85CA2FFA1B08D309L)
    }

    "handle equal arrays of longs" in {
      checkDiff (seq (fixedLong), values, values)
    }

    "handle dropping longs from the array" in {
      checkDiff (seq (fixedLong), values, values.filter (_ != 0x3C4F7468C8B828FEL))
    }

    "handle adding longs to the array" in {
      checkDiff (seq (fixedLong), values filter (_ != 0x056C1999B220A24AL), values)
    }

    "handle changing longs in the array" in {
      checkDiff (
          seq (fixedLong),
          values updated (7, 0x6A973D914A523044L),
          values updated (11, 0xC2E5194FAFF06599L))
    }}}
