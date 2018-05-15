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
package libra.merge.merge;

import java.io.IOException;
import java.util.Hashtable;
import libra.common.hadoop.io.datatypes.IntArrayWritable;
import libra.common.hadoop.io.datatypes.CompressedSequenceWritable;
import libra.common.kmermatch.KmerMatchFileMapping;
import libra.common.kmermatch.KmerMatchResult;
import libra.merge.common.MergeConfig;
import libra.preprocess.common.filetable.FileTable;
import libra.preprocess.common.helpers.KmerIndexHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author iychoi
 */
public class MergeMapper extends Mapper<CompressedSequenceWritable, KmerMatchResult, CompressedSequenceWritable, IntArrayWritable> {
    
    private static final Log LOG = LogFactory.getLog(MergeMapper.class);
    
    private MergeConfig mConfig;
    private FileTable[] fileTables;
    private String[][] samplesInFileTable;
    private KmerMatchFileMapping fileMapping;
    
    private Hashtable<String, Integer> fileTableIDConvTable = new Hashtable<String, Integer>();
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        
        this.mConfig = MergeConfig.createInstance(conf);
        this.fileTables = this.mConfig.getFileTables().toArray(new FileTable[0]);
        this.samplesInFileTable = new String[this.fileTables.length][];
        for(int i=0;i<this.fileTables.length;i++) {
            this.samplesInFileTable[i] = this.fileTables[i].getSamples().toArray(new String[0]);
        }
        
        this.fileMapping = KmerMatchFileMapping.createInstance(conf);
    }
    
    @Override
    protected void map(CompressedSequenceWritable key, KmerMatchResult value, Context context) throws IOException, InterruptedException {
        IntArrayWritable[] valueArray = value.getVals();
        Path[] kmerIndexTablePathArray = value.getKmerIndexTablePaths();
        
        // merge - count
        int countFields = 0;
        for(int i=0;i<valueArray.length;i++) {
            if(valueArray[i] != null) {
                countFields += valueArray[i].get().length;
            }
        }
        
        // merge
        int[] newValueArray = new int[countFields];
        int indexNewValueArray = 0;
        for(int i=0;i<valueArray.length;i++) {
            if(valueArray[i] != null) {
                int[] arr = valueArray[i].get();
                
                Path fileTablePath = kmerIndexTablePathArray[i];
                String fileTableName = KmerIndexHelper.getFileTableName(fileTablePath);
                int fileTableID = convertFileTableNameToFileTableID(fileTableName);

                for(int j=0;j<arr.length/2;j++) {
                    int file_id_in_table = arr[j*2];
                    int freq = arr[j*2 + 1];

                    int file_id = convertFileIDToGlobalFileID(fileTableID, file_id_in_table);
                    
                    newValueArray[indexNewValueArray] = file_id;
                    newValueArray[indexNewValueArray+1] = freq;
                    indexNewValueArray += 2;
                }
            }
        }
        
        context.write(key, new IntArrayWritable(newValueArray));
    }
    
    private int convertFileTableNameToFileTableID(String fileTableName) {
        Integer fileTableID = this.fileTableIDConvTable.get(fileTableName);
        if(fileTableID == null) {
            for(int i=0;i<this.fileTables.length;i++) {
                String name = this.fileTables[i].getName();
                if(name.equals(fileTableName)) {
                    fileTableID = i;
                    this.fileTableIDConvTable.put(name, i);
                    break;
                }
            }
        }
        return fileTableID;
    }
    
    private int convertFileIDToGlobalFileID(int file_table_id, int file_id_in_table) throws IOException {
        String sampleName = this.samplesInFileTable[file_table_id][file_id_in_table];
        return this.fileMapping.getIDFromSampleFile(sampleName);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        
    }
}
