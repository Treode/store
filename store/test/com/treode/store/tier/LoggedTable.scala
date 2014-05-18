package com.treode.store.tier

import com.treode.async.{Async, AsyncIterator, Scheduler}
import com.treode.disk.{Disk, ObjectId, PageHandler}
import com.treode.store.{Bytes, Residents, TxClock}

import Async.{guard, supply}
import TierTestTools._

private class LoggedTable
