/*
 * Copyright 2014 Treode, Inc.
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

package com.treode.store

import java.util.concurrent.ConcurrentHashMap

import com.treode.async.Callback
import com.treode.pickle.PicklerRegistry

package object catalog {

  private [catalog] type Ping = Seq [(CatalogId, Int)]
  private [catalog] type Sync = Seq [(CatalogId, Update)]

  private [catalog] type Proposal = Option [(BallotNumber, Patch)]
  private [catalog] type Learner = Callback [Patch]

  private [catalog] def newAcceptorsMap = new ConcurrentHashMap [(CatalogId, Int), Acceptor]
  private [catalog] def newProposersMap = new ConcurrentHashMap [(CatalogId, Int), Proposer]

  private [catalog] val catalogChunkSize = 16
  private [catalog] val catalogHistoryLimit = 16
}
