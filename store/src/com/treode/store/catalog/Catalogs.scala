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

package com.treode.store.catalog

import scala.util.Random

import com.treode.async.{Async, Scheduler}
import com.treode.cluster.Cluster
import com.treode.disk.{DiskLaunch, DiskRecovery}
import com.treode.store.{CatalogDescriptor, Library, StoreConfig}

private [store] trait Catalogs {

  def listen [C] (desc: CatalogDescriptor [C]) (f: C => Any)

  def issue [C] (desc: CatalogDescriptor [C]) (version: Int, cat: C): Async [Unit]
}

private [store] object Catalogs {

  trait Recovery {

    def launch (implicit disk: DiskLaunch, cluster: Cluster): Async [Catalogs]
  }

  def recover () (implicit
      random: Random,
      scheduler: Scheduler,
      library: Library,
      recovery: DiskRecovery,
      config: StoreConfig
  ): Recovery =
    new RecoveryKit
}
