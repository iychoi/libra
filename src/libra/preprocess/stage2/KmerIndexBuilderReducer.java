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
package libra.preprocess.stage2;

import java.io.IOException;
import libra.common.hadoop.io.datatypes.IntArrayWritable;
import libra.common.hadoop.io.datatypes.CompressedSequenceWritable;
import libra.preprocess.common.PreprocessorRoundConfig;
import libra.preprocess.common.filetable.FileTable;
import libra.preprocess.common.helpers.KmerStatisticsHelper;
import libra.preprocess.common.kmerstatistics.KmerStatisticsPart;
import libra.preprocess.common.kmerstatistics.KmerStatisticsPartTable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author iychoi
 */
public class KmerIndexBuilderReducer extends Reducer<CompressedSequenceWritable, IntArrayWritable, CompressedSequenceWritable, IntArrayWritable> {
    
    private static final Log LOG = LogFactory.getLog(KmerIndexBuilderReducer.class);

    private PreprocessorRoundConfig ppConfig;
    private FileTable fileTable;
    private KmerStatisticsPart[] statisticsParts;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        
        this.ppConfig = PreprocessorRoundConfig.createInstance(conf);
        this.fileTable = this.ppConfig.getFileTable();
        this.statisticsParts = new KmerStatisticsPart[this.ppConfig.getFileTable().samples()];
        
        int idx = 0;
        for(String sample : this.fileTable.getSamples()) {
            this.statisticsParts[idx] = new KmerStatisticsPart(sample);
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
        
        int nonZeroFiles = 0;
        for(int k=0;k<freqTable.length;k++) {
            if(freqTable[k] > 0) {
                nonZeroFiles++;
            }
        }
        
        int[] outputFreqArr = new int[nonZeroFiles*2];
        int idx = 0;
        for(int l=0;l<freqTable.length;l++) {
            if(freqTable[l] > 0) {
                outputFreqArr[idx] = l; // fid
                outputFreqArr[idx+1] = freqTable[l]; // freq
                idx += 2;
            }
        }

        // compute base
        for(int m=0;m<freqTable.length;m++) {
            int frequency = freqTable[m];
            if(frequency > 0) {
                this.statisticsParts[m].incrementLogTFWeight(Math.pow(1 + Math.log10(frequency), 2));
                this.statisticsParts[m].incrementNaturalTFWeight(Math.pow(frequency, 2));
                this.statisticsParts[m].incrementBooleanTFWeight(1);
            }
        }
        
        context.write(key, new IntArrayWritable(outputFreqArr));
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        int taskID = context.getTaskAttemptID().getTaskID().getId();
        
        KmerStatisticsPartTable table = new KmerStatisticsPartTable(this.fileTable.getName());
        for(KmerStatisticsPart statisticsPart : this.statisticsParts) {
            table.addStatisticsPart(statisticsPart);
        }
        
        String statisticsPartTableFileName = KmerStatisticsHelper.makeKmerStatisticsPartTableFileName(this.fileTable.getName(), taskID);

        Path statisticsPartTableOutputFile = new Path(this.ppConfig.getKmerStatisticsPath(), statisticsPartTableFileName);
        FileSystem outputFileSystem = statisticsPartTableOutputFile.getFileSystem(context.getConfiguration());

        table.saveTo(outputFileSystem, statisticsPartTableOutputFile);
    }
}
