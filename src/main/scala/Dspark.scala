import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.util.{Try, Success, Failure}
import fr.inra.lipm.general.paramparser.ArgParser


object Dspark {
  def main(args: Array[String]) = {

    // Spark configuration
    val conf = new SparkConf().setAppName("DSparK")
    val sc = new SparkContext(conf)

    // Param parser
    val parser = new ArgParser()
    parser.addParamString("input", 'i', "Input files or directory")
    parser.addParamString("output", 'o', "Output directory")
    parser.addParamLong("kmer-size", 'k', "Kmer Size", 31)
    parser.addParamLong("abundance-max", 'x', "Maximum abundance", 2147483647)
    parser.addParamLong("abundance-min", 'n', "Minimum abundance", 2)

    parser.parse(args)

    parser.assertPathIsFile("input")
    //TODO: assert output don't exist
    //TODO: assert ksize is odd

    val input = parser.getString("input")
    val output = parser.getString("output")
    val kmerSize = parser.getLong("kmer-size").toInt
    val abundanceMax = parser.getLong("abundance-max")
    val abundanceMin = parser.getLong("abundance-min")

    val sortOrder = Map("A" -> 0, "C" -> 1, "T" -> 2, "G" -> 3)
    val baseComplement = Map("A" -> "T", "C" -> "G", "G" -> "C", "T" -> "A")

    //Broadcast variables on all nodes
    val broadcastedKmerSize = sc.broadcast(kmerSize)
    val broadcastedAbundanceMax = sc.broadcast(abundanceMax)
    val broadcastedAbundanceMin = sc.broadcast(abundanceMin)
    val broadcastedSortOrder = sc.broadcast(sortOrder)
    val broadcastedBaseComplement = sc.broadcast(baseComplement)

    // main
    val tryLines = sc.textFile(input)

    val lines = Try(tryLines.first) match {
      case Success(x) => tryLines
      case Failure(x) => sc.textFile(s"$input/*/*")
    }

    // Remove quality lines if input are fastq
    val firstLine = lines.first()
    val linesWithoutQuality = firstLine.startsWith("@") match {
      case true => {
        val indexedLines = lines.zipWithIndex()
        val indexedLinesWithoutQuality = indexedLines.filter(tpl => (tpl._2 + 1) % 4 != 0)
        indexedLinesWithoutQuality.map(tpl => tpl._1)
      }
      case false => lines
    }

    val reads = lines.filter(line => {
      !(
        line.startsWith("@") ||
          line.startsWith("+") ||
          line.startsWith(";") ||
          line.startsWith("!") ||
          line.startsWith("~") ||
          line.startsWith(">")
        )
    })

    val kmers = reads.flatMap(read => read.sliding(kmerSize, 1))
    val kmersWithoutN = kmers.filter(kmer => !kmer.contains("N"))

    def isCanonical(kmer: String): Boolean = {
      val len = kmer.length
      val start = kmer.substring(0, 1)
      val reversedEnd = broadcastedBaseComplement.value(kmer.substring(len - 1, len))

      if (len <= 3) true

      val subtraction = broadcastedSortOrder.value(reversedEnd) - broadcastedSortOrder.value(start)

      subtraction match {
        case a if a > 0 => true
        case a if a < 0 => false
        case 0 => isCanonical(kmer.substring(1).dropRight(1))
      }
    }

    def revComp(kmer: String): String = {
      kmer.map {
        case 'A' => 'T'
        case 'C' => 'G'
        case 'G' => 'C'
        case 'T' => 'A'
      }.reverse
    }

    def getCanonical(kmer: String): String = {
      if (isCanonical(kmer)) {
        kmer
      } else {
        revComp(kmer)
      }
    }

    val canonicalKmers = kmersWithoutN.map(getCanonical)

    // count kmers
    val countedKmers = canonicalKmers.map((_, 1)).reduceByKey(_ + _)

    // filter on abundance
    val filteredKmers = countedKmers.filter(kmer_tpl => kmer_tpl._2 >= broadcastedAbundanceMin.value && kmer_tpl._2 <= broadcastedAbundanceMax.value)

    filteredKmers.saveAsTextFile(output)
  }
}