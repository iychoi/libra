# libra

[![Build Status](https://travis-ci.org/iychoi/libra.svg?branch=master)](https://travis-ci.org/iychoi/libra)

Compute the Similarity between Metagenomic Samples

BUILD
-----
Use `ANT` build system.

Build without dependent libs
```
ant
```

Build with dependent libs
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
- t : number of tasks (reducers)
- s : min size of group in bytes. 10GB by default. For each file group, a separate index file is created. 
- g : max number of groups. 20 groups by default. If groups to be created by "-s" option exceeds this value, combine groups.
- o : output directory


Scoring
```
hadoop jar libra-all.jar core -m MAP -w LOGALITHM -t 100 -o /score_dir /index_dir
```

Scoring Options
- m : run mode. MAP | REDUCE
- w : weighting algorithm. LOGALITHM | BOOLEAN | NATURAL
- t : number of tasks (input splits at MAP mode, reducers at REDUCE mode)
- o : output directory


