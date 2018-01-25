

class Kmer(kmer:String, broadcastedBaseComplement:org.apache.spark.broadcast.Broadcast[Map[String,String]], broadcastedSortOrder:org.apache.spark.broadcast.Broadcast[Map[String,Int]]) {

  def isCanonical(kmer:String):Boolean = {
    val len = kmer.length
    val start = kmer.substring(0,1)
    val reversedEnd = broadcastedBaseComplement.value(kmer.substring(len - 1, len))

    //if (len >= 3) true

    val subtraction = broadcastedSortOrder.value(reversedEnd) - broadcastedSortOrder.value(start)

    subtraction match {
      case a if a > 0 => true
      case a if a < 0 => false
      case 0 => isCanonical(kmer.substring(1).dropRight(1))
    }
  }

  def revComp():String = {
    kmer.map {
      case 'A' => 'T'
      case 'C' => 'G'
      case 'G' => 'C'
      case 'T' => 'A'
    }.reverse
  }
}
