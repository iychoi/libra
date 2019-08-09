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
import libra.common.hadoop.io.datatypes.CompressedSequenceWritable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 *
 * @author iychoi
 */
public class KmerMatchRecordReader extends RecordReader<CompressedSequenceWritable, KmerMatchResult> {
    
    private static final Log LOG = LogFactory.getLog(KmerMatchRecordReader.class);
    
    private int kmerSize;
    private Path[] indexTableFilePaths;
    private int partitionNo;
    private KmerJoiner joiner;
    private Configuration conf;
    private KmerMatchResult curResult;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        if(!(split instanceof KmerMatchInputSplit)) {
            throw new IOException("split is not an instance of KmerMatchIndexSplit");
        }
        
        KmerMatchInputSplit kmerIndexSplit = (KmerMatchInputSplit) split;
        this.conf = context.getConfiguration();
        this.kmerSize = kmerIndexSplit.getKmerSize();
        this.indexTableFilePaths = kmerIndexSplit.getIndexTableFilePaths();
        this.partitionNo = kmerIndexSplit.getPartitionNo();
        
        this.joiner = new KmerJoiner(this.kmerSize, this.indexTableFilePaths, this.partitionNo, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        this.curResult = this.joiner.stepNext();
        if(this.curResult != null) {
            return true;
        }
        return false;
    }

    @Override
    public CompressedSequenceWritable getCurrentKey() {
        if(this.curResult != null) {
            return this.curResult.getKey();
        }
        return null;
    }

    @Override
    public KmerMatchResult getCurrentValue() {
        return this.curResult;
    }

    @Override
    public float getProgress() throws IOException {
        return this.joiner.getProgress();
    }

    @Override
    public synchronized void close() throws IOException {
        this.joiner.close();
    }
}
