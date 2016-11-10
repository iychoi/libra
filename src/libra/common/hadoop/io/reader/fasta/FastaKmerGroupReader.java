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
package libra.common.hadoop.io.reader.fasta;

import java.io.IOException;
import libra.common.fasta.KmerLines;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 *
 * @author iychoi
 */
public class FastaKmerGroupReader extends RecordReader<LongWritable, KmerLines> {

    private final static int GROUP_SIZE = 50;
    
    private FastaKmerReader rawKmerReader = new FastaKmerReader();
    
    private LongWritable key;
    private KmerLines value;
    
    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return this.key;
    }

    @Override
    public KmerLines getCurrentValue() throws IOException, InterruptedException {
        return this.value;
    }

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context)
            throws IOException, InterruptedException {

        this.rawKmerReader.initialize(genericSplit, context);
        
        this.key = null;
        this.value = null;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        KmerLines lines = new KmerLines(GROUP_SIZE);
        boolean bfirst = true;
        boolean bhasData = false;
        
        this.key = null;
        this.value = null;
        
        for(int i=0;i<GROUP_SIZE;i++) {
            boolean hasNext = this.rawKmerReader.nextKeyValue();
            if(hasNext) {
                LongWritable key = this.rawKmerReader.getCurrentKey();
                Text value = this.rawKmerReader.getCurrentValue();
                
                if(bfirst) {
                    this.key = key;
                    bfirst = false;
                }
                
                lines.set(i, value.toString());
                bhasData = true;
            } else {
                break;
            }
        }
        
        if(bhasData) {
            this.value = lines;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public float getProgress() throws IOException {
        return this.rawKmerReader.getProgress();
    }

    @Override
    public void close() throws IOException {
        this.rawKmerReader.close();
    }
}