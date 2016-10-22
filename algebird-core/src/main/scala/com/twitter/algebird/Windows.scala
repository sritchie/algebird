package com.twitter.algebird

/**
 * Sam's Notes:
 *
 * * Found one bug with +; + was failing with instances of different
 *   lengths. Fixed with a different, probably inefficient `+`
 *   implementation... I bet we could make it faster by reversing the
 *   way we track items in the ring buffer, by advancing backwards?
 */

//          800 900      700
//          |   |   1000 |
// Vector ( 19, 20, 23, 16 )
//    index=2        ^
//    time=1000
//    step=100

// [700, 800): saw a sum of 16
// [800, 900): saw a sum of 19
// [900, 1000): saw a sum of 20
// [1000, 1100): saw at least a sum of 23 *in progress*

// 24 hour moving window, with 30 minute resolution (step)
// (48 + 1 buckets)
//
// time range you can support: step * (slots - 1)

/**
 * We only bump `time` in increments of `step`. `time`
 * references the inclusive lower bound of the bucket you're currently
 * in.
 */
case class Windows[A](buf: RingBuf[A], step: Long, time: Long) { lhs =>
  /**
   * Inclusive lower bound of the supplied slot offset, looking back
   * from the current slot.
   */
  private def minTime: Long = time - (buf.size - 1) * step

  private[this] def stepsFrom(beginning: Long, now: Long): Long =
    ((now - beginning) / step).toLong

  /**
   * Only step forward if nextTime is >= time + step.
   */
  def step(currTime: Long)(implicit ev: Monoid[A]): Windows[A] =
    if (currTime < time + step) this
    else {
      // we do need to slide forward
      val i = stepsFrom(time, currTime)
      require(i >= 1, s"currTime=$currTime, step=$step, time=$time, i=$i")

      // moving forward by i (>= 1) time steps
      val w = Windows(buf.step(i), step, time + (i * step))
      require(currTime < w.time + w.step, s"currTime=$currTime, time=$time, w.time=${w.time}")
      require(currTime >= w.minTime)
      w
    }

  /**
   * Adding something before minTime is a no-op.
   */
  def add(a: A, currTime: Long)(implicit ev: Monoid[A]): Windows[A] =
    if (currTime < minTime) this
    else if (currTime < time + step) {
      copy(buf = buf.add(a, stepsFrom(currTime, time)))
    } else step(currTime).add(a, currTime)

  //   [3,5,6,1]
  // [1,2,4,3]
  //   [5,9,9,1]
  /**
   * - Step both forward in time.
   * - Get an iterator BACK in time from the current index
   * - `zip` will drop the tail of the longer reverseIterator.
   * - reverse again before building the new Windows to maintain the
   *   ring buffer structure, and set the index pointer to the last
   *   item.
   */
  def +(rhs: Windows[A])(implicit ev: Monoid[A]): Windows[A] = {
    require(lhs.step == rhs.step)
    val currTime = lhs.time max rhs.time
    val newBuf = lhs.step(currTime).buf + rhs.step(currTime).buf
    Windows(newBuf, step, currTime)
  }

  def lowerBoundSum(implicit ev: Monoid[A]): A =
    ev.sum(buf.iterator.drop(1))

  def upperBoundSum(implicit ev: Monoid[A]): A =
    ev.sum(buf.slots)
}

object Windows {
  // step=100ms
  // timeNow=53ms
  // adjustedTime=0ms
  def empty[A: Monoid](timeStep: Int, numSteps: Int): Windows[A] =
    Windows(RingBuf.empty[A](numSteps + 1), timeStep, 0L)

  def monoid[A](timeStep: Int, numSteps: Int)(implicit ev: Monoid[A]): Monoid[Windows[A]] =
    Monoid.from(Windows.empty(timeStep, numSteps))(_ + _)

  implicit def equiv[A: Equiv]: Equiv[Windows[A]] =
    new Equiv[Windows[A]] {
      def equiv(x: Windows[A], y: Windows[A]): Boolean =
        (x.step == y.step) &&
          (x.time == y.time) &&
          Equiv[RingBuf[A]].equiv(x.buf, y.buf)
    }
}
