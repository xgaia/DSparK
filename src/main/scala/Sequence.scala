package sequence

import scala.collection.immutable.BitSet


class Sequence(kmerSize: Int) extends java.io.Serializable {

  val nuclMapReverseLong = Map('A' -> 2L, 'T' -> 0L, 'G' -> 1L, 'C' -> 3L)
  val longToChar = List('A', 'C', 'T', 'G')
  val kmerMask = (1L << kmerSize * 2) - 1

  def sequenceToLongCanonicalKmersIterator(iterReads: Iterator[String]): Iterator[(Long, Int)] = {
    iterReads.flatMap(sequenceToLongCanonicalKmers)
  }

  def sequenceToLongCanonicalKmers(sequence: String): Array[(Long, Int)] = {
    // Build the first kmer
    val firstStringKmer = sequence.take(kmerSize)
    val firstBinaryKmerTuple = kmerToLongTuple(firstStringKmer)
    // Get the rest of the sequence
    val restOfSequence = sequence.takeRight(sequence.length - kmerSize)
    // get all kmers with the rest of seq
    val arrayOfKmersLongTuple = extendsArrayOfKmersLongTuple(restOfSequence, Array(firstBinaryKmerTuple))
    // return the canonical kmers
    arrayOfKmersLongTuple.map(getCanonical)
  }

  def nuclToLong(nucl: Char): Long = {
    nucl >> 1 & 3
  }

  def kmerToLongTuple(kmer: String): (Long, Long) = {
    kmerToLongTuple(kmer, kmer.reverse, (0L, 0L))
  }

  def kmerToLongTuple(kmer: String, revKmer: String, longTuple: (Long, Long)): (Long, Long) = {
    val kmerLen = kmer.length
    if (kmerLen == 0) {
      longTuple
    } else {
      val forwardLong = (longTuple._1 << 2) + nuclToLong(kmer.head)
      val reverseLong = (longTuple._2 << 2) + nuclMapReverseLong(revKmer.head)
      kmerToLongTuple(kmer.tail, revKmer.tail, (forwardLong, reverseLong))
    }
  }

  def extendsArrayOfKmersLongTuple(restOfStringSequence: String, arrayOfLongKmersTuple: Array[(Long, Long)]): Array[(Long, Long)] = {
    val lenOfRest = restOfStringSequence.length
    if (lenOfRest == 0) {
      arrayOfLongKmersTuple
    } else {
      val nucl = restOfStringSequence.head
      // forward, add nucl at end of previousforward
      val newForward = ((arrayOfLongKmersTuple.last._1 << 2) + nuclToLong(nucl)) & kmerMask
      // reverse, add nucl at the begening of previous reverse
      val newReverse = (arrayOfLongKmersTuple.last._2 >> 2) | (nuclMapReverseLong(nucl) << (2 * (kmerSize - 1)))
      extendsArrayOfKmersLongTuple(restOfStringSequence.tail, arrayOfLongKmersTuple :+ (newForward, newReverse))
    }
  }

  def getCanonical(longTuple: (Long, Long)): (Long, Int) = {
    (List(longTuple._1, longTuple._2).min, 1)
  }

  // Kmer Conversion Conversion
  def longToString(long: Long): String = {
    longToString(long, "")
  }

  def longToString(long: Long, string: String): String = {
    if (string.length == kmerSize) {
      string
    } else {
      //last nucl
      val lastNucl = longToChar((long & 3).toInt)
      // extends the string
      val kmer = lastNucl + string
      // remove the nucl from the long
      val tail = long >> 2
      longToString(tail, kmer)
    }
  }
}
