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
import libra.common.hadoop.io.datatypes.CompressedSequenceWritable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author iychoi
 */
public class KmerIndexBuilderCombiner extends Reducer<CompressedSequenceWritable, IntWritable, CompressedSequenceWritable, IntWritable> {
    
    private static final Log LOG = LogFactory.getLog(KmerIndexBuilderCombiner.class);
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
    }
    
    @Override
    protected void reduce(CompressedSequenceWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int frequency = 0;
        
        for(IntWritable value : values) {
            frequency += value.get();
        }
        
        context.write(key, new IntWritable(frequency));
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}
