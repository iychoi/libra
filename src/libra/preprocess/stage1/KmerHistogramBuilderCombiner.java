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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author iychoi
 */
public class KmerHistogramBuilderCombiner extends Reducer<IntWritable, IntArrayWritable, IntWritable, IntArrayWritable> {
    
    private static final Log LOG = LogFactory.getLog(KmerHistogramBuilderCombiner.class);
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
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
        
        context.write(key, new IntArrayWritable(freqArrSum));
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}
