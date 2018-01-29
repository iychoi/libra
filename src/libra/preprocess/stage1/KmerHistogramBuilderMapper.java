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
import libra.common.hadoop.io.datatypes.LongArrayWritable;
import libra.common.helpers.SequenceHelper;
import libra.common.sequence.KmerLines;
import libra.preprocess.common.PreprocessorRoundConfig;
import libra.preprocess.common.kmerhistogram.KmerHistogram;
import libra.preprocess.common.kmerhistogram.KmerHistogramRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author iychoi
 */
public class KmerHistogramBuilderMapper extends Mapper<LongWritable, KmerLines, IntWritable, LongArrayWritable> {
    
    private static final Log LOG = LogFactory.getLog(KmerHistogramBuilderMapper.class);
    
    private PreprocessorRoundConfig ppConfig;
    private KmerHistogram histogram;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        this.ppConfig = PreprocessorRoundConfig.createInstance(conf);
        this.histogram = new KmerHistogram(this.ppConfig.getFileTable().getName(), this.ppConfig.getKmerSize());
    }
    
    @Override
    protected void map(LongWritable key, KmerLines value, Context context) throws IOException, InterruptedException {
        for(String line : value.get()) {
            if(line != null) {
                String sequence = line.toUpperCase();
                this.histogram.takeSample(sequence);
            }
        }
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        int samplingCharLen = this.histogram.getSamplingCharLen();
        int arrLen = (int)Math.pow(4, samplingCharLen);
        
        long histoArr[] = new long[arrLen];
        for(int i=0;i<arrLen;i++) {
            histoArr[i] = 0;
        }
        
        for(KmerHistogramRecord record : this.histogram.getRecord()) {
            int idx = SequenceHelper.convertToInteger(record.getKmer());
            histoArr[idx] = record.getFrequency();
        }
        
        context.write(new IntWritable(0), new LongArrayWritable(histoArr));
        
        this.histogram = null;
    }
}
