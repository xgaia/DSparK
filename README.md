# DSparK

DSparK is a Spark implementation of [DSK](https://github.com/gatb/dsk), a kmer counting program.

## Installation

Clone the repository and cd into it

```bash
git clone https://github.com/xgaia/DSparK.git
cd DSparK
```

## Package
```bash
sbt assembly
```

## Usage

Spark parameters (for spark-ics cluster):

- `--master`: `yarn`
- `--deploy-mode`: `cluster`
- `--exec_num`: number of spark executor, max 150
- `--exec_memory`: memory per executor. max 5G


DSparK parameters:

- `--input`: input file (fasta/q) or directory of input files
- `--input-type`: `fasta` or `fastq`
- `--ouptout`: output directory, it must not exist
- `--kmer-size`: the kmer size, must be odd, and maximum is `31` (default `31`)
- `--abundance-min`: min abundance threshold for solid kmers (default `2`)
- `--abundance-max`: max abundance threshold for solid kmers (default `2147483647`)
- `--format`: `binary` or `text`: the output format of kmers (default `binary`)
- `--sorted`: sort results, only if `format` is set to `text`

Example:

```bash
spark-submit --master yarn --deploy-mode cluster --num-executor 150 --executor-memory 5G target/scala-2.11/DSparK-assembly-0.1.jar --input reads.fasta --output counted_kmers
```
