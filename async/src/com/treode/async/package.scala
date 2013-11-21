package com.treode

package object async {

  def toRunnable (task: => Any): Runnable =
    new Runnable {
      def run() = task
    }}
