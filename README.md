# DSparK

DSparK is a Spark implementation of [DSK](https://github.com/gatb/dsk), a kmer counting program.

## Installation

Clone the repository and cd into it

```bash
git clone https://lipm-gitlab.toulouse.inra.fr/spark-ics/DSparK.git
cd DSparK
```

## Package
```bash
sbt assembly
```

## Usage

```bash
spark-submit --master yarn --deploy-mode cluster --num-executor <exec_num> --executor-memory <exec_memory> target/scala-2.11/DSparK-assembly-0.1.jar --input <input> --output <output>
```

With:

- `exec_num`: number of spark executor, up to 150
- `exec_memory`: memory per executor. up to 5G
- `input`: input file (fasta/q) or directory of input files
- `ouptout`: output directory, it must not exist

DSparK take also optional parameters:

- `--kmer-size`: the kmer size (default 31)
- `--abundance-min`: min abundance threshold for solid kmers (default 2)
- `--abundance-max`: max abundance threshold for solid kmers (default 2147483647)
