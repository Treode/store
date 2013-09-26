package com.treode.io.buffer

import org.scalatest.PropSpec
import org.scalatest.prop.PropertyChecks

class StreamSpec extends PropSpec with PropertyChecks {

  property ("A Stream reads and writes bytes") {
    forAll ("x") { x: Byte =>
      val b = Buffer()
      b.writeByte (x)
      expectResult (x) (b.readByte())
    }
  }

  property ("A Stream reads and writes fixed length ints") {
    forAll ("x") { x: Int =>
      val b = Buffer()
      b.writeInt (x)
      expectResult (x) (b.readInt())
    }
  }

  property ("A Stream reads and writes variable length ints") {
    forAll ("x") { x: Int =>
      val b = Buffer()
      b.writeVariableLengthInt (x)
      expectResult (x) (b.readVariableLengthInt())
    }
  }

  property ("A Stream reads and writes variable length unsigned ints") {
    forAll ("x") { x: Int =>
      val b = Buffer()
      b.writeVariableLengthUnsignedInt (x)
      expectResult (x) (b.readVariableLengthUnsignedInt())
    }
  }

  property ("A Stream reads and writes fixed length longs") {
    forAll ("x") { x: Long =>
      val b = Buffer()
      b.writeLong (x)
      expectResult (x) (b.readLong())
    }
  }

  property ("A Stream reads and writes variable length longs") {
    forAll ("x") { x: Long =>
      val b = Buffer()
      b.writeVariableLengthLong (x)
      expectResult (x) (b.readVariableLengthLong())
    }
  }

  property ("A Stream reads and writes variable length unsigned longs") {
    forAll ("x") { x: Long =>
      val b = Buffer()
      b.writeVariableLengthUnsignedLong (x)
      expectResult (x) (b.readVariableLengthUnsignedLong())
    }
  }

  property ("A Stream reads and writes floats") {
    forAll ("x") { x: Float =>
      val b = Buffer()
      b.writeFloat (x)
      expectResult (x) (b.readFloat())
    }
  }

  property ("A Stream reads and writes doubles") {
    forAll ("x") { x: Double =>
      val b = Buffer()
      b.writeDouble (x)
      expectResult (x) (b.readDouble())
    }
  }
}
