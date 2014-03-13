package com.treode

import scala.util.Try

package object async {

  type Callback [A] = Try [A] => Any
}
