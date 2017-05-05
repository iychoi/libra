# libra

[![Build Status](https://travis-ci.org/iychoi/libra.svg?branch=master)](https://travis-ci.org/iychoi/libra)

Compute Similarity between Large Metagenomic Samples

BUILD
-----
Use `ANT` build system.

Build without dependent libs
```
ant allinone
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
hadoop jar libra-all.jar preprocess -k 20 -o /index_dir /source_dir
```

Preprocessing Options
- k : k-mer size
- o : output directory


Scoring
```
hadoop jar libra-all.jar core -Dmapreduce.job.reduces=20 reduce -w LOGALITHM -o /score_dir /index_dir
```

Scoring Options
- w : weighting algorithm. LOGALITHM | BOOLEAN | NATURAL
- o : output directory


