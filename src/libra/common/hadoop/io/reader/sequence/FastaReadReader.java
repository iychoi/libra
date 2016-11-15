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
package libra.common.hadoop.io.reader.sequence;

import java.io.IOException;
import libra.common.sequence.FastaRawRead;
import libra.common.sequence.FastaRead;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 *
 * @author iychoi
 */
public class FastaReadReader extends RecordReader<LongWritable, FastaRead> {

    private FastaRawReadReader rawReadReader = new FastaRawReadReader();
    
    private LongWritable key;
    private FastaRead value;
    
    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return this.key;
    }

    @Override
    public FastaRead getCurrentValue() throws IOException, InterruptedException {
        return this.value;
    }

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context)
            throws IOException, InterruptedException {

        this.rawReadReader.initialize(genericSplit, context);
        
        this.key = null;
        this.value = null;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        boolean retVal = this.rawReadReader.nextKeyValue();
        if(retVal) {
            FastaRawRead value = this.rawReadReader.getCurrentValue();
            if(value != null) {
                FastaRead read = new FastaRead(value);
                this.value = read;
                this.key = new LongWritable(read.getReadOffset());
            } else {
                this.key = null;
                this.value = null;
            }
        } else {
            this.key = null;
            this.value = null;
        }
        
        return retVal;
    }

    @Override
    public float getProgress() throws IOException {
        return this.rawReadReader.getProgress();
    }

    @Override
    public void close() throws IOException {
        this.rawReadReader.close();
    }
}