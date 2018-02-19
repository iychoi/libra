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
package libra.preprocess.stage3;

import java.io.IOException;
import libra.common.hadoop.io.datatypes.IntArrayWritable;
import libra.common.hadoop.io.datatypes.CompressedSequenceWritable;
import libra.preprocess.common.FilterAlgorithm;
import libra.preprocess.common.PreprocessorRoundConfig;
import libra.preprocess.common.filetable.FileTable;
import libra.preprocess.common.helpers.KmerStatisticsHelper;
import libra.preprocess.common.kmerfilter.KmerFilter;
import libra.preprocess.common.kmerfilter.KmerFilterTable;
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
    private int[] allowedFrequencyMin;
    private int[] allowedFrequencyMax;
    private KmerStatisticsPart[] statisticsParts;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        
        this.ppConfig = PreprocessorRoundConfig.createInstance(conf);
        this.fileTable = this.ppConfig.getFileTable();
        
        int sample_size = this.ppConfig.getFileTable().samples();
        // filter
        this.allowedFrequencyMin = new int[sample_size];
        this.allowedFrequencyMax = new int[sample_size];
        
        FilterAlgorithm filterAlgorithm = this.ppConfig.getFilterAlgorithm();
        
        switch(filterAlgorithm) {
            case NONE:
                {
                    for(int i=0;i<sample_size;i++) {
                        this.allowedFrequencyMin[i] = 1;
                        this.allowedFrequencyMax[i] = Integer.MAX_VALUE;
                    }
                }
                break;
            case NOTUNIQUE:
                {
                    for(int i=0;i<sample_size;i++) {
                        this.allowedFrequencyMin[i] = 2;
                        this.allowedFrequencyMax[i] = Integer.MAX_VALUE;
                    }
                }
                break;
            case STDDEV:
                {
                    // read filter
                    KmerFilterTable kmerFilterTable = KmerFilterTable.createInstance(conf);
                    KmerFilter[] filters = kmerFilterTable.getFilter().toArray(new KmerFilter[0]);
                    
                    for(int i=0;i<sample_size;i++) {
                        double mean = filters[i].getMean();
                        double stddev = filters[i].getStddev();
                        this.allowedFrequencyMin[i] = (int) Math.max(0, Math.ceil(mean - stddev));
                        this.allowedFrequencyMax[i] = (int) Math.floor(mean + stddev);
                    }
                }
                break;
            case STDDEV2:
                {
                    // read filter
                    KmerFilterTable kmerFilterTable = KmerFilterTable.createInstance(conf);
                    KmerFilter[] filters = kmerFilterTable.getFilter().toArray(new KmerFilter[0]);
                    
                    for(int i=0;i<sample_size;i++) {
                        double mean = filters[i].getMean();
                        double stddev = filters[i].getStddev() * 2;
                        this.allowedFrequencyMin[i] = (int) Math.max(0, Math.ceil(mean - stddev));
                        this.allowedFrequencyMax[i] = (int) Math.floor(mean + stddev);
                    }
                }
                break;
            default:
                LOG.info("Unknown filter algorithm specified : " + filterAlgorithm.toString());
                throw new IOException("Unknown filter algorithm specified : " + filterAlgorithm.toString());
        }
        
        this.statisticsParts = new KmerStatisticsPart[sample_size];
        
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
        
        // filter
        for(int i=0;i<freqTable.length;i++) {
            if(freqTable[i] < this.allowedFrequencyMin[i]) {
                // out of min boundary
                freqTable[i] = 0;
            } else if(freqTable[i] > this.allowedFrequencyMax[i]) {
                // out of max boundary
                freqTable[i] = 0;
            }
        }
        
        int nonZeroFiles = 0;
        for(int i=0;i<freqTable.length;i++) {
            if(freqTable[i] > 0) {
                nonZeroFiles++;
            }
        }
        
        int[] outputFreqArr = new int[nonZeroFiles*2];
        int idx = 0;
        for(int i=0;i<freqTable.length;i++) {
            if(freqTable[i] > 0) {
                outputFreqArr[idx] = i; // fid
                outputFreqArr[idx+1] = freqTable[i]; // freq
                idx += 2;
            }
        }

        // compute base
        for(int i=0;i<freqTable.length;i++) {
            int frequency = freqTable[i];
            if(frequency > 0) {
                this.statisticsParts[i].incrementLogTFWeightSquare(Math.pow(1 + Math.log10(frequency), 2));
                this.statisticsParts[i].incrementLogTFWeight(1 + Math.log10(frequency));
                this.statisticsParts[i].incrementNaturalTFWeightSquare(Math.pow(frequency, 2));
                this.statisticsParts[i].incrementNaturalTFWeightSquare(frequency);
                this.statisticsParts[i].incrementBooleanTFWeight(1);
            }
        }
        
        if(nonZeroFiles > 0) {
            context.write(key, new IntArrayWritable(outputFreqArr));
        }
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
