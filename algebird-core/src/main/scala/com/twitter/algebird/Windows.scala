package com.twitter.algebird

import scala.annotation.tailrec

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

// time range you can support: step * (slots - 1)
case class Windows[A](slots: Vector[A], index: Int, step: Long, time: Long) { lhs =>

  // oldest-to-newest iterator across partial sums
  def iterator: Iterator[A] =
    (1 to slots.size).iterator.map(i => slots(makeIndex(index + i)))

  @inline private[this] def makeIndex(i: Long): Int =
    ((i + slots.size) % slots.size).toInt

  private[this] def clear(vect: Vector[A], i: Int)(implicit ev: Monoid[A]): Vector[A] =
    vect.updated(i, ev.zero)

  def step(currTime: Long)(implicit ev: Monoid[A]): Windows[A] =
    if (currTime < time + step) {
      // we don't need to slide forward
      this
    } else {
      // we do need to slide forward
      val delta = currTime - time
      val i = (delta / step).toLong
      val nextIndex: Int = makeIndex(index + i)
      val w = if (i == 1) {
        // moving forward by 1 time step
        Windows(clear(slots, nextIndex), nextIndex, step, time + step)
      } else {
        require(i > 1, s"currTime=$currTime, step=$step, time=$time, delta=$delta, i=$i")
        // moving forward by i (>= 2) time steps
        val vect = (1L to (i min slots.size)).foldLeft(slots) { (acc, j) =>
          clear(acc, makeIndex(index + j))
        }
        Windows(vect, nextIndex, step, time + (i * step))
      }
      require(currTime < w.time + w.step, s"currTime=$currTime, time=$time, w.time=${w.time}")
      require(currTime >= w.minTime)
      w
    }

  def minTime: Long =
    time - (slots.size - 1) * step

  private[this] def slotContains(i: Int, currTime: Long): Boolean = {
    // i=4, index=5, indexDelta=1
    // i=6, index=5, indexDelta=slots.size - 1
    val n = slots.size
    val indexDelta = ((index - i) + n) % n
    val start = time - indexDelta * step
    start <= currTime && currTime < (start + step)
  }

  // invariant: currTime must in [minTime, time + step).
  private[this] def getSlot(currTime: Long): Int = {
    require(minTime <= currTime)
    require(currTime < time + step)
    @tailrec def loop(thisIndex: Int): Int =
      if (slotContains(thisIndex, currTime)) thisIndex
      else loop(makeIndex(thisIndex - 1))
    loop(index)
  }

  def add(a: A, currTime: Long)(implicit ev: Monoid[A]): Windows[A] =
    if (currTime < minTime) this
    else if (currTime < time + step) {
      val i = getSlot(currTime)
      val sum = ev.plus(slots(i), a)
      copy(slots = slots.updated(i, sum))
    } else step(currTime).add(a, currTime)

  def lowerBoundSum(implicit ev: Monoid[A]): A = {
    val before = ev.sum(slots.iterator.take(index - 1)) // before
    val after = ev.sum(slots.iterator.drop(index)) // after
    ev.plus(before, after)
  }

  def upperBoundSum(implicit ev: Monoid[A]): A =
    ev.sum(slots)

  //   [3,5,6,1]
  // [1,2,4,3]
  //   [5,9,9,1]
  def +(rhs: Windows[A])(implicit ev: Monoid[A]): Windows[A] = {
    require(lhs.step == rhs.step)
    val currTime = lhs.time max rhs.time
    val lhs2 = lhs.step(currTime)
    val rhs2 = rhs.step(currTime)
    val bldr = Vector.newBuilder[A]

    (0 until (lhs2.slots.size min rhs2.slots.size)).foreach { i =>
      val li = makeIndex(lhs2.index + i)
      val ri = makeIndex(rhs2.index + i)
      val lx = lhs2.slots(li)
      val rx = rhs2.slots(ri)
      val zx = ev.plus(lx, rx)
      bldr += zx
    }
    Windows(bldr.result(), 0, lhs2.step, currTime)
  }
}

object Windows {

  // step=100ms
  // timeNow=53ms
  // adjustedTime=0ms
  def empty[A](timeStep: Int, numSteps: Int)(implicit ev: Monoid[A]): Windows[A] =
    Windows(Vector.fill(numSteps + 1)(ev.zero), 0, timeStep, 0L)

  def monoid[A](timeStep: Int, numSteps: Int)(implicit ev: Monoid[A]): Monoid[Windows[A]] =
    new Monoid[Windows[A]] {
      def zero: Windows[A] =
        Windows.empty(timeStep, numSteps)
      def plus(x: Windows[A], y: Windows[A]): Windows[A] =
        x + y
    }

  implicit def equiv[A](implicit ev: Equiv[A]): Equiv[Windows[A]] =
    new Equiv[Windows[A]] {
      def equiv(x: Windows[A], y: Windows[A]): Boolean =
        (x.step == y.step) &&
          (x.time == y.time) &&
          (x.iterator zip y.iterator).forall { case (m, n) => ev.equiv(m, n) }
    }
}
