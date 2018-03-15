import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.rdd.RDDFunctions._
//import scala.util.{Failure, Success, Try}
import fr.inra.lipm.general.paramparser.ArgParser


object Dspark {
  def main(args: Array[String]) = {

    // Spark configuration
    val conf = new SparkConf().setAppName("DSparK")
    val sc = new SparkContext(conf)

    // Param parser
    val parser = new ArgParser()
    parser.addParamString("input", 'i', "Input files or directory")
    parser.addParamString("input-type", 't', "Input type")
    parser.addParamString("output", 'o', "Output directory")
    parser.addParamLong("kmer-size", 'k', "Kmer Size", Some(31))
    parser.addParamLong("abundance-max", 'x', "Maximum abundance", Some(2147483647))
    parser.addParamLong("abundance-min", 'n', "Minimum abundance", Some(2))
    parser.addParamCounter("sorted", 's')
    parser.addParamCounter("format", 'f')

    parser.parse(args)

    // TODO: assertion (don't work well in Spark, filesystem are different)
    parser.assertAllowedValue("input-type", Array("fasta", "fastq"))
    //parser.assertPathIsFile("input")

    val input = parser.getString("input")
    val inputType = parser.getString("input-type")
    val output = parser.getString("output")
    val kmerSize = parser.getLong("kmer-size").toInt
    val abundanceMax = parser.getLong("abundance-max")
    val abundanceMin = parser.getLong("abundance-min")
    val sorted = parser.getCounter("sorted")
    val format = parser.getCounter("format")

    val sortOrder = Map('A' -> 0, 'C' -> 1, 'T' -> 2, 'G' -> 3)
    val baseComplement = Map('A' -> 'T', 'C' -> 'G', 'G' -> 'C', 'T' -> 'A')

    //Broadcast variables on all nodes
    val broadcastedKmerSize = sc.broadcast(kmerSize)
    val broadcastedAbundanceMax = sc.broadcast(abundanceMax)
    val broadcastedAbundanceMin = sc.broadcast(abundanceMin)
    val broadcastedSortOrder = sc.broadcast(sortOrder)
    val broadcastedBaseComplement = sc.broadcast(baseComplement)

    // main --------------------------
    val reads = inputType match {
      case "fasta" => sc.textFile(input).sliding(2, 2).map{case Array(id, seq) => seq}
      case "fastq" => sc.textFile(input).sliding(4, 4).map{case Array(id, seq, _, qual) => seq}
    }

    val kmers = reads.flatMap(read => read.sliding(kmerSize, 1))
    val kmersWithoutN = kmers.filter(kmer => !kmer.contains("N"))

    def getCanonicalIterator(iterKmer: Iterator[String]): Iterator[String] = {

      def isCanonical(kmer: String): Boolean = {
        val len = kmer.length
        val start = kmer.head
        val reversedEnd = broadcastedBaseComplement.value(kmer.charAt(len - 1))

        if (len <= 3) true

        val subtraction = broadcastedSortOrder.value(reversedEnd) - broadcastedSortOrder.value(start)

        subtraction match {
          case a if a > 0 => true
          case a if a < 0 => false
          case 0 => isCanonical(kmer.substring(1).dropRight(1))
        }
      }

      def revComp(kmer: String): String = {
        kmer.map(broadcastedBaseComplement.value(_)).reverse
      }

      def getCanonical(kmer: String): String = {
        if (isCanonical(kmer)) {
          kmer
        } else {
          revComp(kmer)
        }
      }

      iterKmer.map(getCanonical)

    }

    val canonicalKmers = kmersWithoutN.mapPartitions(getCanonicalIterator)

    // count kmers
    val countedKmers = canonicalKmers.map((_, 1)).reduceByKey(_ + _)

    // filter on abundance
    val filteredKmers = countedKmers.filter(kmer_tpl => kmer_tpl._2 >= broadcastedAbundanceMin.value && kmer_tpl._2 <= broadcastedAbundanceMax.value)

    // Sort
    val sortedKmers = sorted match {
      case 0 => filteredKmers
      case _ => filteredKmers.sortByKey()
    }

    // format
    val formatedKmers = format match {
      case 0 => sortedKmers
      case _ => sortedKmers.map(x => x.toString().replace("(", "").replace(")", "").replace(",", " "))
    }

    formatedKmers.saveAsTextFile(output)
  }
}
