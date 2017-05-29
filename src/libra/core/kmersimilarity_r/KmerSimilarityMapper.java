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
import java.util.Collection;
import libra.common.hadoop.io.datatypes.CompressedIntArrayWritable;
import libra.common.hadoop.io.datatypes.CompressedSequenceWritable;
import libra.common.kmermatch.KmerMatchFileMapping;
import libra.core.commom.CoreConfig;
import libra.preprocess.common.filetable.FileTable;
import libra.preprocess.common.helpers.KmerIndexHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 *
 * @author iychoi
 */
public class KmerSimilarityMapper extends Mapper<CompressedSequenceWritable, CompressedIntArrayWritable, CompressedSequenceWritable, CompressedIntArrayWritable> {
    
    private static final Log LOG = LogFactory.getLog(KmerSimilarityMapper.class);
    
    private CoreConfig cConfig;
    private FileTable fileTable;
    private String[] samplesInFileTable;
    private KmerMatchFileMapping fileMapping;
    
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        
        this.cConfig = CoreConfig.createInstance(conf);
        FileSplit inputSplit = (FileSplit)context.getInputSplit();
        String fileTableName = KmerIndexHelper.getFileTableName(inputSplit.getPath().getParent().getName());
        for(FileTable table : this.cConfig.getFileTable()) {
            if(table.getName().equals(fileTableName)) {
                this.fileTable = table;
                break;
            }
        }
        
        Collection<String> samples = this.fileTable.getSamples();
        this.samplesInFileTable = samples.toArray(new String[0]);
        
        this.fileMapping = KmerMatchFileMapping.createInstance(conf);
        
    }
    
    @Override
    protected void map(CompressedSequenceWritable key, CompressedIntArrayWritable value, Context context) throws IOException, InterruptedException {
        int[] value_arr = value.get();
        
        for(int i=0;i<value_arr.length/2;i++) {
            int file_id_in_table = value_arr[i*2];
            int freq = value_arr[i*2 + 1];
            
            int file_id = convertFileIDToGlobalFileID(file_id_in_table);
            value_arr[i*2] = file_id;
        }
        
        context.write(key, new CompressedIntArrayWritable(value_arr));
    }
    
    private int convertFileIDToGlobalFileID(int file_id_in_table) throws IOException {
        String sampleName = this.samplesInFileTable[file_id_in_table];
        return this.fileMapping.getIDFromSequenceFile(sampleName);
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}
