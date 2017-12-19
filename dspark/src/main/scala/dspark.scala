import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.util.Try

object DSpark {

    // Main method
    def main(args: Array[String]) {

        val usage = """Usage: <input file or dir> <results dir>"""


        if (args.length == 0) {
            println(usage)
            sys.exit(1)
        }


        // Prepare SparkContext
        val conf = new SparkConf().setAppName("kmer counting application")
        val sc = new SparkContext(conf)

        // Contants
        val base_complement = Map("A" -> "T", "C" -> "G", "G" -> "C", "T" -> "A")
        val bc_base_complement = sc.broadcast(base_complement)

        val sort_order = Map("A" -> 0, "C" -> 1, "T" -> 2, "G" -> 3)
        val bc_sort_order = sc.broadcast(sort_order)

        val ksize = 31
        val bc_ksize = sc.broadcast(ksize)

        val max_shift = ksize/2+1
        val bc_max_shift = sc.broadcast(max_shift)

        val abudance_min = 2
        val bc_abudance_min = sc.broadcast(abudance_min)

        val abudance_max = 2147483647
        val bc_abudance_max = sc.broadcast(abudance_max)


        def get_canonical_kmer(kmer: String) : String = {
            var num = 0
            var first = kmer.substring(0, 1)
            var last = bc_base_complement.value(kmer.takeRight(1))
            while(first == last) {
                num += 1
                if (num == bc_max_shift.value){
                    return kmer
                }
                first = kmer.substring(0+num, 1+num)
                last = kmer.substring(ksize-num-2, ksize-num-1)
            }

            if(bc_sort_order.value(first) < bc_sort_order.value(last)) {
                return kmer
            }

            return kmer.map {
                case 'A' => 'T'
                case 'C' => 'G'
                case 'G' => 'C'
                case 'T' => 'A'
            }.reverse
        }


        // get all files into a uniq RDD
        var lines = sc.textFile(s"${args(0)}")

        // If path is a dir of dir of files, get recursivly the files
        val test = Try(lines.first)
        if (test.isFailure) {
            lines = sc.textFile(s"${args(0)}/*/*")
        }

        val first_line = lines.take(1)(0)

        if (first_line.startsWith("@")) {
            // fastq, remove quality line
            val indexed_lines = lines.zipWithIndex()
            val indexed_lines_without_quality = indexed_lines.filter(tpl => ((tpl _2)+1)%4 != 0)
            lines = indexed_lines_without_quality.map(tpl => (tpl _1))
        }

        // Remove non sequences lines
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

        // Get kmers
        val kmers = reads.flatMap(read => read.sliding(bc_ksize.value, 1))
        // Remove kmer who contains N
        val kmers_without_N = kmers.filter(kmer => ! kmer.contains("N"))
        // Get canonical kmers
        val canonical_kmers = kmers_without_N.map(get_canonical_kmer(_))
        // create tuple
        val kmer_tpl = canonical_kmers.map(kmer => (kmer, 1))
        // count kmers
        val counted_kmers = kmer_tpl.reduceByKey(_ + _)
        // Remove kmer who don't respect abudance
        val final_result = counted_kmers.filter(kmer_tpl => (kmer_tpl _2) >= bc_abudance_min.value).filter(kmer_tpl => (kmer_tpl _2) <= bc_abudance_max.value)

        // Write result
        final_result.saveAsTextFile(s"${args(1)}")

    }
}

