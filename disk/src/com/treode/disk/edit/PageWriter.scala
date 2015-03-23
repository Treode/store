/* Copyright 2014 Treode, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.treode.disk.edit
import com.treode.async.{Async, Callback, Scheduler}, Async.supply, Callback.ignore
import com.treode.async.io.File
import com.treode.async.Async
import com.treode.buffer.PagedBuffer
import scala.collection.mutable.UnrolledBuffer
import com.treode.async.implicits._
import com.treode.disk.Position
import com.treode.async.{Async, Callback, Scheduler}, Async.async
import com.treode.disk.PickledPage



/*
    PageWriters get items to write from a PageDispatcher and
    write them to a file.
 */
private class PageWriter(dsp: PageDispatcher,val file: File)
 {
  val bits = 10
  val buffer = PagedBuffer (bits)
  // we maintain an internal pointer into the file for where to write
  // in order to preserve consistency between writes.
  var pos : Long = buffer.writePos 
  

  // we consistently check to see if we have anything to write.
  listen() run (ignore)


  /*
    This polls the attached PageDispatcher for items to write to the
    attached file.
   */
  def listen(): Async[Unit] =
    for {
      (_, dataBuffers) <- dsp.receive()
      _ <- write(dataBuffers)
    } yield {
      listen() run (ignore)
    }

    /*
      Write all of the PickledPages data in the UnrolledBuffer 
      to the file, then return the position where the PickledPages were
      written to the callback inside the PickledPage.   
     */
   def write(data: UnrolledBuffer[PickledPage]): Async [Unit] = {
    var writePositions : Array[Position] = new Array[Position](0);
    var beforeAnyWrites = pos
    for (s <- data) {
      val beforeEachWritePos = buffer.writePos
      s.write(buffer)
      val writeLen : Long = (buffer.writePos - beforeEachWritePos).toLong
      writePositions :+= (Position(0,pos.toLong, writeLen.toInt))//only focus on one disk for now
      pos += writeLen
    }
    for {
      _ <- file.flush (buffer, beforeAnyWrites)
    } yield {
      buffer.clear ()
      for ((s,q) <- data zip writePositions){
        s.cb.pass(q)
      }
    }
  }

}
