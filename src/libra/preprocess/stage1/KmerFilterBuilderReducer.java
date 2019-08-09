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
package libra.preprocess.stage1;

import java.io.IOException;
import libra.common.hadoop.io.datatypes.IntArrayWritable;
import libra.common.hadoop.io.datatypes.CompressedSequenceWritable;
import libra.preprocess.common.PreprocessorRoundConfig;
import libra.preprocess.common.filetable.FileTable;
import libra.preprocess.common.helpers.KmerFilterHelper;
import libra.preprocess.common.kmerfilter.KmerFilterPart;
import libra.preprocess.common.kmerfilter.KmerFilterPartTable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author iychoi
 */
public class KmerFilterBuilderReducer extends Reducer<CompressedSequenceWritable, IntArrayWritable, NullWritable, NullWritable> {
    
    private static final Log LOG = LogFactory.getLog(KmerFilterBuilderReducer.class);

    private PreprocessorRoundConfig ppConfig;
    private FileTable fileTable;
    private KmerFilterPart[] filterParts;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        
        this.ppConfig = PreprocessorRoundConfig.createInstance(conf);
        this.fileTable = this.ppConfig.getFileTable();
        this.filterParts = new KmerFilterPart[this.ppConfig.getFileTable().samples()];
        
        int idx = 0;
        for(String sample : this.fileTable.getSamples()) {
            this.filterParts[idx] = new KmerFilterPart(sample);
            idx++;
        }
    }
    
    @Override
    protected void reduce(CompressedSequenceWritable key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {
        int[] freqTable = new int[this.fileTable.samples()];
        for(int i=0;i<freqTable.length;i++) {
            freqTable[i] = 0;
        }
        
        for(IntArrayWritable value : values) {
            int[] v_arr = value.get();
            
            if(v_arr.length % 2 != 0) {
                throw new IOException("Input record is not valid");
            }
            
            for(int j=0;j<v_arr.length/2;j++) {
                int fid = v_arr[j*2];
                int freq = v_arr[j*2 + 1];
                
                freqTable[fid] += freq;
            }
        }

        // compute base
        for(int m=0;m<freqTable.length;m++) {
            int frequency = freqTable[m];
            if(frequency > 0) {
                this.filterParts[m].incrementTotalKmers(frequency);
                this.filterParts[m].incrementUniqueKmers(1);
                this.filterParts[m].incrementSumOfSquare(Math.pow(frequency, 2));
            }
        }
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        int taskID = context.getTaskAttemptID().getTaskID().getId();
        
        KmerFilterPartTable table = new KmerFilterPartTable(this.fileTable.getName());
        for(KmerFilterPart filterPart : this.filterParts) {
            table.addFilterPart(filterPart);
        }
        
        String filterPartTableFileName = KmerFilterHelper.makeKmerFilterPartTableFileName(this.fileTable.getName(), taskID);

        Path filterPartTableOutputFile = new Path(this.ppConfig.getKmerFilterPath(), filterPartTableFileName);
        FileSystem outputFileSystem = filterPartTableOutputFile.getFileSystem(context.getConfiguration());

        table.saveTo(outputFileSystem, filterPartTableOutputFile);
    }
}
