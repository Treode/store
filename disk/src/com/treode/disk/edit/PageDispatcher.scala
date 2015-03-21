/*
 * Copyright 2014 Treode, Inc.
 *In Disk.scala, read is
def read [P] (desc: PageDescriptor [P], pos: Position): Async [P]
it's not a special method for pickled pages. it is the method. you should no longer have a read without a descriptor
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

import com.treode.async.io.File
import com.treode.async.Async
import com.treode.buffer.PagedBuffer
import com.treode.disk.Dispatcher
import scala.collection.mutable.UnrolledBuffer
import com.treode.async.{Async, Callback, Fiber, Scheduler }, Async.async
import com.treode.disk.PickledPage
import com.treode.disk.PageDescriptor
import scala.reflect.ClassTag
import com.treode.disk.Position
import com.treode.pickle.Pickler
import com.treode.disk.ObjectId


import com.treode.async.{Async, Callback, Scheduler}




/**	
	A page dispatcher schedules writes to be written to a disk.


	Schedules a PageDescriptor to be written to disk using
	implicit scheduler passed.
 **/
private class PageDispatcher(implicit scheduler: Scheduler)
 extends Dispatcher [PickledPage] {

/*
	Schedules a PageDescriptor with an objectId, generation, and page
	to be written as a pickled object to disk
	Returns Position object where PageDescriptor was written
 */
def write [P] (desc: PageDescriptor [P], obj: ObjectId, gen: Long, page: P): 
				Async [Position] =
	async { cb => 
		send (PickledPage (desc, obj, gen, page, cb))
	}
}