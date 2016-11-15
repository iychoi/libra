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
package libra.core.kmersimilarity_r;

import java.io.IOException;
import libra.common.hadoop.io.datatypes.CompressedIntArrayWritable;
import libra.common.hadoop.io.datatypes.CompressedSequenceWritable;
import libra.common.kmermatch.KmerMatchFileMapping;
import libra.preprocess.common.helpers.KmerIndexHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 *
 * @author iychoi
 */
public class KmerSimilarityMapper extends Mapper<CompressedSequenceWritable, IntWritable, CompressedSequenceWritable, CompressedIntArrayWritable> {
    
    private static final Log LOG = LogFactory.getLog(KmerSimilarityMapper.class);
    
    private KmerMatchFileMapping fileMapping;
    private int file_id = 0;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.fileMapping = KmerMatchFileMapping.createInstance(context.getConfiguration());
        
        FileSplit inputSplit = (FileSplit)context.getInputSplit();
        
        String sequenceFilename = KmerIndexHelper.getSequenceFileName(inputSplit.getPath().getParent().getName());
        
        this.file_id = this.fileMapping.getIDFromSequenceFile(sequenceFilename);
    }
    
    @Override
    protected void map(CompressedSequenceWritable key, IntWritable value, Context context) throws IOException, InterruptedException {
        int[] arr = new int[2];
        arr[0] = this.file_id;
        arr[1] = value.get();
        
        context.write(key, new CompressedIntArrayWritable(arr));
    }
        
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        this.fileMapping = null;
    }
}
