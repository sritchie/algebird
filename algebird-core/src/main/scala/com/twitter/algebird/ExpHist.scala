package com.twitter.algebird

/**
 * Exponential Histogram algorithm from
 * http://ilpubs.stanford.edu:8090/504/1/2001-34.pdf.
 *
 *
 * Next step - code up the l-canonical representation of numbers,
 * then do some of the fancier business in the paper!
 */
object ExpHist {
  case class Config(k: Int, windowSize: Long) {
    // Maximum number of buckets of size 2^i allowed in the repr of
    // this exponential histogram.
    def maxBuckets: Int = math.ceil(k / 2.0).toInt + 1

    // Returns the last timestamp before the window. any ts <= [the
    // returned timestamp] is outside the window.
    def expiration(currTime: Long): Long = currTime - windowSize
  }

  def empty(k: Int, windowSize: Long): ExpHist =
    ExpHist(Config(k, windowSize), Vector.empty, 0L)
}

case class ExpHist(conf: ExpHist.Config, buckets: Vector[BucketSeq], time: Long) {
  /**
   * Returns the same ExpHist with a new window. If the new window is
   * smaller than the current window, evicts older items.
   */
  def withWindow(newWindow: Long): ExpHist =
    copy(conf = conf.copy(windowSize = newWindow)).step(time)

  /**
   * Step forward to `newTime`, evicting any wrapped buckets that
   * fall outside of the window.
   */
  def step(newTime: Long): ExpHist =
    if (newTime <= time) this
    else {
      val t = conf.expiration(newTime)
      copy(
        buckets = buckets.flatMap(_.expire(t)),
        time = newTime)
    }

  /**
   * "inc" algorithm:
   *
   *  * Step forward to the new timestamp
   *  * create a new bucket with size 1 and timestamp
   *  * Traverse the list of buckets in order of increasing sizes.
   *
   * If there are (k / 2 + 2) or more buckets of the same size, merge
   * the oldest two of these buckets into a single bucket of double
   * the size. Repeat until the bucket size is less than the limit.
   */
  def inc(timestamp: Long): ExpHist = {
    val newBuckets =
      buckets.lastOption match {
        case None => Vector(BucketSeq.one(timestamp))
        case Some(bucketSeq) => buckets.init :+ (bucketSeq + timestamp)
      }
    step(timestamp).copy(buckets = BucketSeq.normalize(conf.maxBuckets, newBuckets))
  }

  // Stupid implementation of `add` - just inc a bunch of times with
  // the same timestamp.
  def add(i: Long, timestamp: Long): ExpHist =
    (0L until i).foldLeft(this) {
      case (acc, _) => acc.inc(timestamp)
    }

  // TODO: Convert these to constructor params, compute as we go.
  // counter for the total size of all buckets.
  def slowTotal: Long = buckets.map(_.count).reduceLeftOption(_ + _).getOrElse(0L)
  // returns the size of the oldest bucket.
  def slowLast: Long = buckets.headOption.map(_.bucketSize).getOrElse(0L)

  def lowerBoundSum: Long = slowTotal - slowLast
  def upperBoundSum: Long = slowTotal

  // For testing. Returns the vector of bucket sizes from largest to
  // smallest.
  def windows: Vector[Long] =
    for {
      b <- buckets
      t <- b.timestamps
    } yield b.bucketSize
}

/**
 * "compressed" representation of `timestamps.length` buckets of size
 * `exp`.
 *
 * Bucket timestamps are sorted in time-increasing order (so the
 * oldest bucket is at the head).
 */
case class BucketSeq(exp: BucketSeq.Pow2, timestamps: Vector[Long]) { l =>
  require(timestamps.sorted == timestamps)
  require(timestamps.nonEmpty)

  // bucketSize, as defined in the paper.
  def bucketSize: Long = exp.value

  // Total number of ticks recorded in this BucketSeq.
  def count: Long = bucketSize * length

  // Total number of buckets tracked by the BucketSeq.
  def length: Int = timestamps.length

  // Add a new timestamp. TODO: NOT sure if it's legit to not require
  // that the new timestamp be > all existing timestamps.
  def +(ts: Long): BucketSeq = copy(timestamps = (timestamps :+ ts).sorted)

  def ++(r: BucketSeq): BucketSeq = {
    require(l.exp == r.exp)
    copy(timestamps = (l.timestamps ++ r.timestamps).sorted)
  }

  /**
   * Remove all timestamps <= the cutoff. Returns:
   *
   *  - None if the resulting [[BucketSeq]] is empty,
   *  - Some(the filtered [[BucketSeq]]) otherwise.
   */
  def expire(cutoff: Long): Option[BucketSeq] =
    Some(copy(timestamps = timestamps.filter(_ > cutoff)))
      .filter(_.timestamps.isEmpty)

  /**
   * Returns the number of pairs to drop to get this BucketSeq's final
   * length <= the supplied limit.
   */
  private[this] def pairsToDrop(limit: Int): Int =
    0 max math.ceil((length - limit) / 2.0).toInt

  /**
   * if this.length <= limit, returns (this, None).
   *
   * else, splits this BucketSeq into two by combining pairs of
   * timestamps into new buckets until enough timestamps have been
   * removed that this.length <= limit.
   *
   * The new pairs form a new [[BucketSeq]] instance with a doubled
   * bucketSize.
   *
   * The return value in this case is a pair of the new, slimmed-down
   * current BucketSeq and Some(the new, doubled BucketSeq).
   */
  def evolve(limit: Int): (BucketSeq, Option[BucketSeq]) =
    pairsToDrop(limit) match {
      case 0 => (this, None)
      case pairs =>
        val childTs = (0 until pairs).map(i => timestamps(i * 2))
        val child = Some(BucketSeq(exp.double, childTs.toVector))
        copy(timestamps = timestamps.drop(pairs * 2)) -> child
    }

  /**
   * Expands this BucketSeq out into a vector of BucketSeq instances
   * that all have lengths <= the supplied limit. (Used on the oldest
   * bucket after normalizing a sequence of BucketSeqs.)
   */
  def expand(limit: Int): Vector[BucketSeq] =
    evolve(limit) match {
      case (bs, None) => Vector(bs)
      case (bs, Some(remaining)) => remaining.expand(limit) :+ bs
    }

  // A little janky, but lets us see nice strings of powers of 2.
  override def toString: String = List.fill(length)(bucketSize).mkString(", ")
}

object BucketSeq {
  case class Pow2(exp: Int) extends AnyVal { l =>
    def double: Pow2 = Pow2(exp + 1)
    def value: Long = (2 << exp) / 2
    def *(r: Pow2) = Pow2(l.exp + r.exp)
  }

  // Returns a singleton bucketseq.
  def one(timestamp: Long): BucketSeq = BucketSeq(Pow2(0), Vector(timestamp))

  // evolves all BucketSeq instances in `seqs` from newest -> oldest
  // until every [[BucketSeq]] length is <= the supplied limit.
  def normalize(limit: Int, seqs: Vector[BucketSeq]): Vector[BucketSeq] = {
    val empty = (Vector.empty[BucketSeq], None: Option[BucketSeq])

    val (ret, extra) = seqs.foldRight(empty) {
      case (bs, (acc, optCarry)) =>
        val (evolved, carry) =
          optCarry.map(_ ++ bs).getOrElse(bs).evolve(limit)
        (evolved +: acc, carry)
    }

    extra.map(_.expand(limit) ++ ret).getOrElse(ret)
  }
}
