package com.treode

package object concurrent {

  def toRunnable (task: => Any): Runnable =
    new Runnable {
      def run() = task
    }}
