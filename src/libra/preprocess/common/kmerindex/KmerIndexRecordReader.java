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
package libra.preprocess.common.kmerindex;

import java.io.IOException;
import java.math.BigInteger;
import libra.common.hadoop.io.datatypes.CompressedSequenceWritable;
import libra.common.helpers.SequenceHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 *
 * @author iychoi
 */
public class KmerIndexRecordReader extends RecordReader<CompressedSequenceWritable, IntWritable> {
    private Path[] inputIndexPaths;
    private Configuration conf;
    private AKmerIndexReader indexReader;
    private BigInteger currentProgress;
    private BigInteger progressEnd;
    private KmerIndexInputFormatConfig inputFormatConfig;
    private CompressedSequenceWritable curKey;
    private IntWritable curVal;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        if(!(split instanceof KmerIndexSplit)) {
            throw new IOException("split is not an instance of KmerIndexSplit");
        }
        
        KmerIndexSplit kmerIndexSplit = (KmerIndexSplit) split;
        this.conf = context.getConfiguration();
        this.inputIndexPaths = kmerIndexSplit.getIndexFilePaths();
        
        this.inputFormatConfig = KmerIndexInputFormatConfig.createInstance(this.conf);
        
        FileSystem fs = this.inputIndexPaths[0].getFileSystem(this.conf);
        this.indexReader = new KmerIndexReader(fs, new Path(this.inputFormatConfig.getKmerIndexIndexPath()), this.conf);
        
        this.currentProgress = BigInteger.ZERO;
        StringBuilder endKmer = new StringBuilder();
        for(int i=0;i<this.inputFormatConfig.getKmerSize();i++) {
            endKmer.append("T");
        }
        this.progressEnd = SequenceHelper.convertToBigInteger(endKmer.toString());
        
        this.curKey = null;
        this.curVal = null;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        CompressedSequenceWritable key = new CompressedSequenceWritable();
        IntWritable val = new IntWritable();
        boolean result = this.indexReader.next(key, val);
        
        if(result) {
            this.curKey = key;
            this.curVal = val;
        } else {
            this.curKey = null;
            this.curVal = null;
        }
        return result;
    }

    @Override
    public CompressedSequenceWritable getCurrentKey() throws IOException, InterruptedException {
        return this.curKey;
    }

    @Override
    public IntWritable getCurrentValue() throws IOException, InterruptedException {
        return this.curVal;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        BigInteger divided = this.currentProgress.multiply(BigInteger.valueOf(100)).divide(this.progressEnd);
        float f = divided.floatValue() / 100;

        return Math.min(1.0f, f);
    }

    @Override
    public void close() throws IOException {
        this.indexReader.close();
    }
}
