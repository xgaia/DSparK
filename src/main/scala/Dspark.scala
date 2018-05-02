import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.rdd.RDDFunctions._
import org.apache.spark.sql.functions.udf
//import scala.util.{Failure, Success, Try}
import fr.inra.lipm.general.paramparser.ArgParser


object Dspark {
  def main(args: Array[String]) = {

    // Spark configuration
    val conf = new SparkConf().setAppName("DSparK")
    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder()
      .appName("DSparK")
      .getOrCreate()

    import spark.implicits._


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
    val broadcastedHalphKmerSize = sc.broadcast((kmerSize / 2) + 1)
    val broadcastedAbundanceMax = sc.broadcast(abundanceMax)
    val broadcastedAbundanceMin = sc.broadcast(abundanceMin)
    val broadcastedSortOrder = sc.broadcast(sortOrder)
    val broadcastedBaseComplement = sc.broadcast(baseComplement)

    // main --------------------------
    val reads = inputType match {
      case "fasta" => sc.textFile(input).sliding(2, 2).map { case Array(id, seq) => seq }
      case "fastq" => sc.textFile(input).sliding(4, 4).map { case Array(id, seq, _, qual) => seq }
    }

    // Df of kmers without N
    val kmers = reads.flatMap(read => read.sliding(kmerSize, 1)).toDF("kmer").filter(!$"kmer".contains("N"))


    def isCanonical(kmer: String, reverseKmer: String): Boolean = {

      val sub = broadcastedSortOrder.value(broadcastedBaseComplement.value(reverseKmer.head)) - broadcastedSortOrder.value(kmer.head)

      sub match {
        case 0 => isCanonical(kmer.tail, reverseKmer.tail)
        case a if a > 0 => true
        case a if a < 0 => false
      }
    }

    def getCanonical(kmer: String): String = {
      // reverse seq
      val revKmer = kmer.reverse
      if (isCanonical(kmer, revKmer)) {
        kmer
      } else {
        // complement
        revKmer.map(broadcastedBaseComplement.value(_))
      }
    }

    def gc: (String => String) = { str => getCanonical(str) }

    val getCanonicalUDF = udf(gc)


    // get canonical
    val canonicalKmers = kmers.withColumn("canonical kmer", getCanonicalUDF(kmers("kmer"))).select("canonical kmer")

    // count kmers
    val result = canonicalKmers.groupBy("canonical kmer").count.filter($"count" >= abundanceMin).filter($"count" <= abundanceMax)


    // Sort
    val sortedKmers = sorted match {
      case 0 => result
      case _ => result.sort("canonical kmer")
    }


    sortedKmers.write.format("csv").save(output)

  }
}