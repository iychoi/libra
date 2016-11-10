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
import libra.common.algorithms.KmerKeySelection.KmerRecord;
import libra.common.hadoop.io.datatypes.CompressedSequenceWritable;
import libra.common.helpers.SequenceHelper;
import libra.preprocess.common.PreprocessorConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author iychoi
 */
public class KmerIndexBuilderMapper extends Mapper<LongWritable, Text, CompressedSequenceWritable, IntWritable> {
    
    private static final Log LOG = LogFactory.getLog(KmerIndexBuilderMapper.class);
    
    private PreprocessorConfig ppConfig;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        
        this.ppConfig = PreprocessorConfig.createInstance(conf);
    }
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String sequence = value.toString();
        
        for (int i = 0; i < (sequence.length() - this.ppConfig.getKmerSize() + 1); i++) {
            String kmer = sequence.substring(i, i + this.ppConfig.getKmerSize());
            if(!SequenceHelper.isValidSequence(kmer)) {
                LOG.info("discard invalid kmer sequence : " + kmer);
                continue;
            }
            
            KmerRecord kmerRecord = new KmerRecord(kmer);
            KmerRecord keyRecord = kmerRecord.getSelectedKey();
            
            context.write(new CompressedSequenceWritable(keyRecord.getSequence()), new IntWritable(1));
        }
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}
