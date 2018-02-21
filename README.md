libra

[![Build Status](https://travis-ci.org/iychoi/libra.svg?branch=master)](https://travis-ci.org/iychoi/libra)

Compute the Similarity between Metagenomic Samples

DOWNLOAD BINARY
---------------
Download a pre-built binary (compiled with Java 7, including dependencies):
- [Libra v1.2 stable](https://github.com/iychoi/libra/releases/download/v1.2/libra-all.jar)

For old releases, check out the release page:
- [Libra Releases](https://github.com/iychoi/libra/releases)


BUILD FROM SOURCE
-----------------
**Most users do not need to build a binary from source. Use pre-built binaries.**

To build, use [ANT](ant.apache.org/) build system.

Type following to build without dependencies:
```
ant
```

Type following to build with dependencies (recommended):
```
ant allinone
```

The `jar` package built will be located at the `/dist` directory.


RUN
---

Preprocessing FASTA/FASTQ files
```
hadoop jar libra-all.jar preprocess -k 20 -t 8 -o /index_dir /source_dir
```

Preprocessing Options
- k : k-mer size
- t : number of tasks (reducers). 1 by default.
- s : min size of group in bytes. 10GB by default. For each file group, a separate index file is created.
- g : max number of groups. 20 groups by default. If groups to be created by "-s" option exceeds this value, combine groups.
- f : kmer filter algorithm. NONE | STDDEV (standard deviation) | STDDEV2 (two's standard deviation) | NOTUNIQUE (default)
- o : output directory


Scoring
```
hadoop jar libra-all.jar core -m MAP -w LOGARITHM -t 8 -o /score_dir /index_dir
```

Scoring Options
- s : scoring algorithm. COSINESIMILARITY (default) | BRAYCURTIS | JENSENSHANNON
- m : run mode. MAP | REDUCE
- w : weighting algorithm. LOGARITHM (default) | BOOLEAN | NATURAL
- t : number of tasks (input splits at MAP mode, reducers at REDUCE mode)
- o : output directory
