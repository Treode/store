package com.treode.store.disk2

import scala.reflect.ClassTag
import com.treode.pickle.Pickler

class PageDescriptor [G, P] (val id: TypeId, val pgrp: Pickler [G], val ppag: Pickler [P]) (
    implicit val tpag: ClassTag [P])
