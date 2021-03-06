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
package libra.distancematrix.kmersimilarity_r;

import java.io.IOException;
import libra.common.hadoop.io.datatypes.IntArrayWritable;
import libra.common.hadoop.io.datatypes.CompressedSequenceWritable;
import libra.common.kmermatch.KmerMatchFileMapping;
import libra.distancematrix.common.DistanceMatrixConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author iychoi
 */
public class KmerSimilarityCombiner extends Reducer<CompressedSequenceWritable, IntArrayWritable, CompressedSequenceWritable, IntArrayWritable> {
    
    private static final Log LOG = LogFactory.getLog(KmerSimilarityCombiner.class);
    
    private DistanceMatrixConfig dmConfig;
    private KmerMatchFileMapping fileMapping;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        
        this.dmConfig = DistanceMatrixConfig.createInstance(conf);
        this.fileMapping = KmerMatchFileMapping.createInstance(conf);
    }
    
    @Override
    protected void reduce(CompressedSequenceWritable key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {
        int[] freqTable = new int[this.fileMapping.getSize()];
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
        
        context.write(key, new IntArrayWritable(outputFreqArr));
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}
