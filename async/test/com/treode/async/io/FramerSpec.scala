package com.treode.async.io

import scala.collection.mutable.Builder

import com.treode.async.{CallbackCaptor, StubScheduler}
import com.treode.buffer.{BufferUnderflowException, PagedBuffer}
import com.treode.pickle.Picklers
import org.scalatest.FreeSpec

class FramerSpec extends FreeSpec {

  val strategy = new Framer.Strategy [Int] {
    def idByteSize = 4
    def newEphemeralId = ???
    def isEphemeralId (id: Int) = false
    def readId (buf: PagedBuffer) = buf.readInt()
    def writeId (id: Int, buf: PagedBuffer) = buf.writeInt (id)
  }

  val p1 = Picklers.int
  val p2 = Picklers.string

  def mkFramer = {
    val framer = new Framer [Int, Unit] (strategy)
    val b1 = Seq.newBuilder [Int]
    val b2 = Seq.newBuilder [String]
    framer.register (p1, 1) (b1 += _)
    framer.register (p2, 2) (b2 += _)
    (framer, b1, b2)
  }

  def mkFile = {
    val scheduler = StubScheduler.random()
    val file = new StubFile (scheduler)
    val buf = PagedBuffer (12)
    (scheduler, file, buf)
  }

  "The Framer should" - {

    "handle direct sends" in {
      val (framer, b1, b2) = mkFramer
      for (i <- 0 to 5)
        if ((i & 1) == 0)
          framer.send (p1, 1, i) (_ => ())
        else
          framer.send (p2, 2, i.toString) (_ => ())
      expectResult (Seq (0, 2, 4)) (b1.result)
      expectResult (Seq ("1", "3", "5")) (b2.result)
    }

    "pass through unrecognized id" in {
      val (framer, b1, b2) = mkFramer
      intercept [FrameNotRecognizedException] {
        framer.send (p1, 3, 0) (_ => throw new Exception)
      }}}

  "With buffers the Framer should" - {

    "handle reading" in {
      val buf = PagedBuffer (12)
      for (i <- 0 to 5)
        if ((i & 1) == 0)
          strategy.write (p1, 1, i, buf)
        else
          strategy.write (p2, 2, i.toString, buf)

      val (framer, b1, b2) = mkFramer
      for (i <- 0 to 5)
        framer.read (buf, _ => ())
      expectResult (Seq (0, 2, 4)) (b1.result)
      expectResult (Seq ("1", "3", "5")) (b2.result)
    }

    "skip over unrecognized ids" in {
      val buf = PagedBuffer (12)
      strategy.write (p1, 0, 0, buf)
      strategy.write (p1, 1, 1, buf)
      val (framer, b1, b2) = mkFramer
      intercept [FrameNotRecognizedException] {
        framer.read (buf, _ => ())
      }
      framer.read (buf, _ => ())
      expectResult (Seq (1)) (b1.result)
    }

    "pass through buffer underflow at the beginning" in {
      val buf = PagedBuffer (12)
      val (framer, b1, b2) = mkFramer
      intercept [BufferUnderflowException] {
        framer.read (buf, _ => ())
      }}

    "pass through buffer underflow after the length" in {
      val buf = PagedBuffer (12)
      buf.writeInt (12)
      val (framer, b1, b2) = mkFramer
      intercept [BufferUnderflowException] {
        framer.read (buf, _ => ())
      }}

    "pass through buffer underflow after the id" in { pending
      val buf = PagedBuffer (12)
      buf.writeInt (12)
      buf.writeInt (1)
      val (framer, b1, b2) = mkFramer
      intercept [BufferUnderflowException] {
        framer.read (buf, _ => ())
      }}}

  "With files the Framer should" - {

    "handle reading" in {
      val (scheduler, file, buf) = mkFile
      for (i <- 0 to 5)
        if ((i & 1) == 0)
          strategy.write (p1, 1, i, buf)
        else
          strategy.write (p2, 2, i.toString, buf)
      file.flush (buf, 0)
      buf.clear()

      val (framer, b1, b2) = mkFramer
      for (i <- 0 to 5) {
        val cb = new CallbackCaptor [Unit]
        framer.read (file, 0, buf, _ => (), cb)
        scheduler.runTasks()
        cb.passed
      }
      expectResult (Seq (0, 2, 4)) (b1.result)
      expectResult (Seq ("1", "3", "5")) (b2.result)
    }

    "skip over unrecognized ids" in {
      val (scheduler, file, buf) = mkFile
      strategy.write (p1, 0, 0, buf)
      strategy.write (p1, 1, 1, buf)
      file.flush (buf, 0)
      buf.clear()
      val (framer, b1, b2) = mkFramer
      var cb = new CallbackCaptor [Unit]
      framer.read (file, 0, buf, _ => (), cb)
      scheduler.runTasks()
      cb.failed.isInstanceOf [FrameNotRecognizedException]
      cb = new CallbackCaptor [Unit]
      framer.read (file, buf.readPos, buf, _ => (), cb)
      scheduler.runTasks()
      cb.passed
      expectResult (Seq (1)) (b1.result)
    }

    "pass through EOF at the beginning" in {
      val (scheduler, file, buf) = mkFile
      val (framer, b1, b2) = mkFramer
      val cb = new CallbackCaptor [Unit]
      framer.read (file, 0, buf, _ => (), cb)
      scheduler.runTasks()
      cb.failed .isInstanceOf [BufferUnderflowException]
    }

    "pass through EOF after the length" in {
      val (scheduler, file, buf) = mkFile
      buf.writeInt (12)
      file.flush (buf, 0)
      buf.clear()
      val (framer, b1, b2) = mkFramer
      val cb = new CallbackCaptor [Unit]
      framer.read (file, 0, buf, _ => (), cb)
      scheduler.runTasks()
      cb.failed .isInstanceOf [BufferUnderflowException]
    }

    "pass through EOF after the id" in {
      val (scheduler, file, buf) = mkFile
      buf.writeInt (12)
      buf.writeInt (1)
      file.flush (buf, 0)
      buf.clear()
      val (framer, b1, b2) = mkFramer
      val cb = new CallbackCaptor [Unit]
      framer.read (file, 0, buf, _ => (), cb)
      scheduler.runTasks()
      cb.failed .isInstanceOf [BufferUnderflowException]
    }}}
