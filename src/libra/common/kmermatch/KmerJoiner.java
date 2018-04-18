/*
 * Copyright 2016 iychoi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package libra.common.kmermatch;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import libra.common.hadoop.io.datatypes.IntArrayWritable;
import libra.common.hadoop.io.datatypes.CompressedSequenceWritable;
import libra.common.helpers.SequenceHelper;
import libra.preprocess.common.kmerhistogram.KmerRangePartition;
import libra.preprocess.common.kmerindex.AKmerIndexReader;
import libra.preprocess.common.kmerindex.KmerIndexReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 *
 * @author iychoi
 */
public class KmerJoiner {
    
    private static final Log LOG = LogFactory.getLog(KmerJoiner.class);
    
    private Path[] kmerIndexTableFilePaths;
    private KmerRangePartition partition;
    private Configuration conf;
    
    private AKmerIndexReader[] readers;
    private BigInteger partitionSize;
    private CompressedSequenceWritable progressKey;
    private boolean eof;
    private BigInteger beginKey;
    
    private CompressedSequenceWritable[] stepKeys;
    private IntArrayWritable[] stepVals;
    private List<Integer> stepMinKeys;
    private boolean stepStarted;
    
    public KmerJoiner(Path[] kmerIndexTableFilePaths, KmerRangePartition partition, TaskAttemptContext context) throws IOException {
        initialize(kmerIndexTableFilePaths, partition, context.getConfiguration());
    }
    
    public KmerJoiner(Path[] kmerIndexTableFilePaths, KmerRangePartition partition, Configuration conf) throws IOException {
        initialize(kmerIndexTableFilePaths, partition, conf);
    }
    
    private void initialize(Path[] kmerIndexTableFilePaths, KmerRangePartition partition, Configuration conf) throws IOException {
        this.kmerIndexTableFilePaths = kmerIndexTableFilePaths;
        this.partition = partition;
        this.conf = conf;
        
        this.readers = new AKmerIndexReader[this.kmerIndexTableFilePaths.length];
        for(int i=0;i<this.readers.length;i++) {
            FileSystem fs = this.kmerIndexTableFilePaths[i].getFileSystem(this.conf);
            this.readers[i] = new KmerIndexReader(fs, this.kmerIndexTableFilePaths[i], this.partition.getPartitionBeginKmer(), this.partition.getPartitionEndKmer(), this.conf);
        }
        
        this.partitionSize = partition.getPartitionSize();
        this.progressKey = null;
        this.eof = false;
        this.beginKey = this.partition.getPartitionBegin();
        this.stepKeys = new CompressedSequenceWritable[this.readers.length];
        this.stepVals = new IntArrayWritable[this.readers.length];
        this.stepStarted = false;
        
        LOG.info("Matcher is initialized");
        LOG.info("> Range " + this.partition.getPartitionBeginKmer() + " ~ " + this.partition.getPartitionEndKmer());
        LOG.info("> Num of Slice Entries : " + this.partition.getPartitionSize().longValue());
    }
    
    public KmerMatchResult stepNext() throws IOException {
        List<Integer> minKeyIndexes = getNextMinKeys();
        if(minKeyIndexes.size() > 0) {
            CompressedSequenceWritable minKey = this.stepKeys[minKeyIndexes.get(0)];
            this.progressKey = minKey;
            
            // check matching
            IntArrayWritable[] minVals = new IntArrayWritable[minKeyIndexes.size()];
            Path[] minIndexPaths = new Path[minKeyIndexes.size()];

            int valIdx = 0;
            for (int idx : minKeyIndexes) {
                minVals[valIdx] = this.stepVals[idx];
                minIndexPaths[valIdx] = this.readers[idx].getKmerIndexTablePath();
                valIdx++;
            }

            return new KmerMatchResult(minKey, minVals, minIndexPaths);
        }
        
        // step failed and no match
        this.eof = true;
        this.progressKey = null;
        return null;
    }
    
    private List<Integer> findMinKeys() throws IOException {
        CompressedSequenceWritable minKey = null;
        List<Integer> minKeyIndice = new ArrayList<Integer>();
        for(int i=0;i<this.readers.length;i++) {
            if(this.stepKeys[i] != null) {
                if(minKey == null) {
                    minKey = this.stepKeys[i];
                    minKeyIndice.clear();
                    minKeyIndice.add(i);
                } else {
                    int comp = minKey.compareTo(this.stepKeys[i]);
                    if (comp == 0) {
                        // found same min key
                        minKeyIndice.add(i);
                    } else if (comp > 0) {
                        // found smaller one
                        minKey = this.stepKeys[i];
                        minKeyIndice.clear();
                        minKeyIndice.add(i);
                    }
                }
            }
        }

        return minKeyIndice;
    }
    
    private List<Integer> getNextMinKeys() throws IOException {
        if(!this.stepStarted) {
            for(int i=0;i<this.readers.length;i++) {
                // fill first
                CompressedSequenceWritable key = new CompressedSequenceWritable();
                IntArrayWritable val = new IntArrayWritable();
                if(this.readers[i].next(key, val)) {
                    this.stepKeys[i] = key;
                    this.stepVals[i] = val;
                } else {
                    this.stepKeys[i] = null;
                    this.stepVals[i] = null;
                }
            }
            
            this.stepStarted = true;
            this.stepMinKeys = findMinKeys();
            return this.stepMinKeys;
        } else {
            // find min key
            if(this.stepMinKeys.size() == 0) {
                //EOF
                return this.stepMinKeys;
            }
            
            // move min pointers
            for (int idx : this.stepMinKeys) {
                CompressedSequenceWritable key = new CompressedSequenceWritable();
                IntArrayWritable val = new IntArrayWritable();
                if(this.readers[idx].next(key, val)) {
                    this.stepKeys[idx] = key;
                    this.stepVals[idx] = val;
                } else {
                    this.stepKeys[idx] = null;
                    this.stepVals[idx] = null;
                }
            }
            
            this.stepMinKeys = findMinKeys();
            return this.stepMinKeys;
        }
    }
    
    public float getProgress() {
        if(this.progressKey == null) {
            if(this.eof) {
                return 1.0f;
            } else {
                return 0.0f;
            }
        } else {
            BigInteger seq = SequenceHelper.convertToBigInteger(this.progressKey.getSequence());
            BigInteger prog = seq.subtract(this.beginKey);
            int comp = this.partitionSize.compareTo(prog);
            if (comp <= 0) {
                return 1.0f;
            } else {
                BigDecimal progDecimal = new BigDecimal(prog);
                BigDecimal rate = progDecimal.divide(new BigDecimal(this.partitionSize), 3, BigDecimal.ROUND_HALF_UP);
                
                float f = rate.floatValue();
                return Math.min(1.0f, f);
            }
        }
    }
    
    public void close() throws IOException {
        for(AKmerIndexReader reader : this.readers) {
            reader.close();
        }
    }
}
