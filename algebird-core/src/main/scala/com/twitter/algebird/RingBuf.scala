package com.twitter.algebird

case class Idx(value: Int) extends AnyVal {
  def +(i: Int): Idx = Idx(value + i)
  def -(i: Int): Idx = Idx(value - i)
}

object Idx {
  def bounded(i: Long, bound: Int): Idx = Idx(((i + bound) % bound).toInt)
}

case class RingBuf[A](slots: Vector[A], index: Int) { lhs =>
  import RingBuf._

  /**
   * Slots are 0-indexed from the current index, looking backwards.
   *
   * `makeIdx` converts an index into a vector position in `slots`.
   *
   * i=1, index=5, makeIdx(i)=Idx(4)
   * i=(slots.size - 1), index=5, makeIdx(i)==Idx(6) (ie last item)
   */
  private[this] def makeIdx(i: Long): Idx = Idx.bounded(i, slots.size)

  // TODO - this assertion is a LITTLE janky, since we want to be able
  // to bump forward during step.
  private[this] def toInternal(i: Long): Idx = {
    assert(i >= 0 && i < slots.size, s"$i is out of allowed bounds: [0 $slots.size)")
    makeIdx(i + index)
  }

  /**
   * Returns a copy of the RingBuf with `a` added in to the proper
   * externally-indexed slot.
   */
  def add(a: A, i: Long)(implicit ev: Monoid[A]): RingBuf[A] = {
    val idx = toInternal(i).value
    copy(slots = slots.updated(idx, ev.plus(slots(idx), a)))
  }

  /**
   * Look up the item in the vector by external index.
   */
  def apply(i: Long): A = slots(toInternal(i).value)

  /**
   * Steps the RingBuf forward `i` steps, zero-ing out all new
   * entries.
   */
  def step(i: Long)(implicit ev: Monoid[A]): RingBuf[A] = {
    if (i <= 0) this
    else {
      val v = (1L to (i min slots.size)).foldLeft(slots) { (acc, j) =>
        acc.updated(makeIdx(j + index).value, ev.zero)
      }
      RingBuf(v, makeIdx(i + index).value)
    }
  }

  def +(rhs: RingBuf[A])(implicit ev: Monoid[A]): RingBuf[A] = {
    val merge = (lhs.reverseIterator zip rhs.reverseIterator).map {
      case (l, r) => ev.plus(l, r)
    }
    val newSize = lhs.slots.size min rhs.slots.size
    RingBuf(merge.toVector.reverse, makeIdx(newSize - 1).value)
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

object RingBuf {
  def empty[A](size: Int)(implicit ev: Monoid[A]): RingBuf[A] =
    RingBuf(Vector.fill(size)(ev.zero), 0)

  def monoid[A](size: Int)(implicit ev: Monoid[A]): Monoid[RingBuf[A]] =
    Monoid.from(RingBuf.empty(size))(_ + _)

  implicit def equiv[A: Equiv](implicit ev: Equiv[A]): Equiv[RingBuf[A]] =
    new Equiv[RingBuf[A]] {
      def equiv(x: RingBuf[A], y: RingBuf[A]): Boolean =
        (x.iterator zip y.iterator).forall { case (m, n) => ev.equiv(m, n) }
    }
}
