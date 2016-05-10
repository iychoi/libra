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
package libra.preprocess.indexing.stage3;

import java.io.IOException;
import libra.common.hadoop.io.datatypes.CompressedSequenceWritable;
import libra.preprocess.common.helpers.KmerIndexHelper;
import libra.preprocess.common.helpers.KmerStatisticsHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 *
 * @author iychoi
 */
public class KmerStatisticsBuilderMapper extends Mapper<CompressedSequenceWritable, IntWritable, NullWritable, NullWritable> {
    
    private static final Log LOG = LogFactory.getLog(KmerStatisticsBuilderMapper.class);
    
    private Counter logTFSquareCounter;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit inputSplit = (FileSplit)context.getInputSplit();
        String fastaFileName = KmerIndexHelper.getFastaFileName(inputSplit.getPath().getParent());
        
        this.logTFSquareCounter = context.getCounter(KmerStatisticsHelper.getCounterGroupNameLogTFSquare(), fastaFileName);
    }
    
    @Override
    protected void map(CompressedSequenceWritable key, IntWritable value, Context context) throws IOException, InterruptedException {
        int frequency = value.get();
        if(frequency > 0) {
            this.logTFSquareCounter.increment((long) (Math.pow(1 + Math.log10(frequency), 2) * 1000));
        }
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}
