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
import libra.common.helpers.SequenceHelper;
import libra.preprocess.common.PreprocessorRoundConfig;
import libra.preprocess.common.helpers.KmerHistogramHelper;
import libra.preprocess.common.kmerhistogram.KmerHistogram;
import libra.preprocess.common.kmerhistogram.KmerHistogramRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author iychoi
 */
public class KmerHistogramBuilderReducer extends Reducer<IntWritable, IntArrayWritable, NullWritable, NullWritable> {
    
    private static final Log LOG = LogFactory.getLog(KmerHistogramBuilderReducer.class);
    
    private PreprocessorRoundConfig ppConfig;
    private KmerHistogram histogram;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        
        this.ppConfig = PreprocessorRoundConfig.createInstance(conf);
        this.histogram = new KmerHistogram(this.ppConfig.getFileTable().getName(), this.ppConfig.getKmerSize());
    }
    
    @Override
    protected void reduce(IntWritable key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {
        int[] freqArrSum = null;
        
        for(IntArrayWritable value : values) {
            int[] freqArr = value.get();
            
            if(freqArrSum == null) {
                freqArrSum = freqArr;
            } else {
                for(int i=0;i<freqArrSum.length;i++) {
                    long eval = (long)freqArrSum[i] + (long)freqArr[i];
                    freqArrSum[i] = (int)Math.min(Integer.MAX_VALUE, eval);
                }
            }
        }
        
        for(int i=0;i<freqArrSum.length;i++) {
            if(freqArrSum[i] > 0) {
                String kmer = SequenceHelper.convertToString(i, this.histogram.getSamplingPrefixLen());
                this.histogram.addRecord(new KmerHistogramRecord(kmer, freqArrSum[i]));
            }
        }
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        String histogramName = this.histogram.getName();
        String histogramFileName = KmerHistogramHelper.makeKmerHistogramFileName(histogramName);

        LOG.info("creating a k-mer histogram file : " + histogramFileName);
        Path histogramOutputFile = new Path(this.ppConfig.getKmerHistogramPath(), histogramFileName);
        FileSystem outputFileSystem = histogramOutputFile.getFileSystem(context.getConfiguration());

        this.histogram.saveTo(outputFileSystem, histogramOutputFile);
    }
}
