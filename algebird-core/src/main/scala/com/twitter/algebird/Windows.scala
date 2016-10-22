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

case class Idx(value: Int) extends AnyVal {
  def +(i: Int): Idx = Idx(value + i)
  def -(i: Int): Idx = Idx(value - i)
}

object Idx {
  def bounded(i: Long, bound: Int): Idx =
    Idx(((i + bound) % bound).toInt)
}

object RingBuf {
  private[RingBuf] def clear[A](v: Vector[A], i: Idx)(implicit ev: Monoid[A]): Vector[A] = v.updated(i.value, ev.zero)
}

case class RingBuf[+A](slots: Vector[A], index: Int) {
  import RingBuf._

  /**
   * Slots are 0-indexed from the current index, looking backwards.
   *
   * `makeIndex` converts an index into a vector position in `slots`.
   *
   * i=1, index=5, makeIdx(i)=Idx(4)
   * i=(slots.size - 1), index=5, makeIdx(i)==Idx(6) (ie last item)
   */
  private[this] def makeIdx(i: Long): Idx =
    Idx.bounded(i, slots.size)

  private[this] def internalAdd(idx: Idx, a: A)(implicit ev: Monoid[A]): RingBuf[A] = {
    val sum = ev.plus(slots(idx.value), a)
    copy(slots = slots.updated(idx.value, sum))
  }

  /**
   * Look up the item in the vector by external index.
   */
  def apply(i: Long): A = {
    assert(i >= 0 && i < slots.size)
    slots(makeIdx(i + index).value)
  }

  // oldest-to-newest iterator across partial sums
  def iterator: Iterator[A] =
    (1 to slots.size).iterator.map(i => slots(makeIdx(index + i).value))

  // newest-to-oldest iterator across partial sums
  def reverseIterator: Iterator[A] =
    (0 until slots.size).iterator.map(i => slots(makeIdx(index - i).value))

  def size: Int = slots.size
  def length: Int = slots.length
}

// time range you can support: step * (slots - 1)
case class Windows[A](buf: RingBuf[A], step: Long, time: Long) { lhs =>
  /**
   * TO KILL
   */
  // TODO kill this, impl detail
  @inline private[this] def makeIndex(i: Long): Int =
    ((i + buf.size) % buf.size).toInt

  // This moves up to Ring Buf.
  private[this] def clear(vect: Vector[A], i: Int)(implicit ev: Monoid[A]): Vector[A] =
    vect.updated(i, ev.zero)

  // ## Slot/Time Mappings

  // Inclusive lower bound of the supplied slot offset, looking back
  // from the current slot.
  private def minTime: Long = timeOf(buf.size - 1)

  @inline private[this] def timeOf(i: Int): Long = time - i * step

  private[this] def stepsFrom(beginning: Long, now: Long): Long =
    ((now - beginning) / step).toLong

  private[this] def slotContains(i: Int, currTime: Long): Boolean =
    stepsFrom(currTime, time) == i

  /**
   * Remaining Implementation
   */

  /**
   * We only actually step in increments of `step`. `time` references
   * the inclusive lower bound of the bucket you're currently in.
   *
   * {{{
   * assert(makeIndex(time % step) == step)
   * }}}
   */
  def step(currTime: Long)(implicit ev: Monoid[A]): Windows[A] =
    if (currTime < time + step) {
      // we don't need to slide forward
      this
    } else {
      // we do need to slide forward
      val i = stepsFrom(time, currTime)
      val nextIndex: Int = makeIndex(buf.index + i)

      require(i >= 1, s"currTime=$currTime, step=$step, time=$time, i=$i")

      val vect = (1L to (i min buf.size)).foldLeft(buf.slots) { (acc, j) =>
        clear(acc, makeIndex(buf.index + j))
      }

      // moving forward by i (>= 2) time steps
      val w = Windows(RingBuf(vect, nextIndex), step, time + (i * step))

      require(currTime < w.time + w.step, s"currTime=$currTime, time=$time, w.time=${w.time}")
      require(currTime >= w.minTime)
      w
    }

  def add(a: A, currTime: Long)(implicit ev: Monoid[A]): Windows[A] =
    if (currTime < minTime) this
    else if (currTime < time + step) {
      val i = stepsFrom(currTime, time)
      val sum = ev.plus(buf(i), a)
      copy(buf = buf.copy(slots = buf.slots.updated(makeIndex(i), sum)))
    } else step(currTime).add(a, currTime)

  def lowerBoundSum(implicit ev: Monoid[A]): A =
    ev.sum(buf.iterator.drop(1))

  def upperBoundSum(implicit ev: Monoid[A]): A =
    ev.sum(buf.slots)

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
    val lhs2 = lhs.step(currTime)
    val rhs2 = rhs.step(currTime)
    val newV = (lhs2.buf.reverseIterator zip rhs2.buf.reverseIterator).map {
      case (l, r) => ev.plus(l, r)
    }
    val newEnd = lhs.buf.slots.size min rhs.buf.slots.size
    Windows(RingBuf(newV.toVector.reverse, makeIndex(newEnd - 1)), lhs2.step, currTime)
  }
}

object Windows {
  // step=100ms
  // timeNow=53ms
  // adjustedTime=0ms
  def empty[A](timeStep: Int, numSteps: Int)(implicit ev: Monoid[A]): Windows[A] =
    Windows(RingBuf(Vector.fill(numSteps + 1)(ev.zero), 0), timeStep, 0L)

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
          (x.buf.iterator zip y.buf.iterator).forall { case (m, n) => ev.equiv(m, n) }
    }
}
