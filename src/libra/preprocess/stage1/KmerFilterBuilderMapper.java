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
import libra.common.hadoop.io.datatypes.CompressedSequenceWritable;
import libra.common.helpers.SequenceHelper;
import libra.common.sequence.ReadInfo;
import libra.preprocess.common.PreprocessorRoundConfig;
import libra.preprocess.common.filetable.FileTable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 *
 * @author iychoi
 */
public class KmerFilterBuilderMapper extends Mapper<LongWritable, ReadInfo, CompressedSequenceWritable, IntArrayWritable> {
    
    private static final Log LOG = LogFactory.getLog(KmerFilterBuilderMapper.class);
    
    private PreprocessorRoundConfig ppConfig;
    private int kmerSize;
    private FileTable fileTable;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        
        this.ppConfig = PreprocessorRoundConfig.createInstance(conf);
        this.kmerSize = this.ppConfig.getKmerSize();
        this.fileTable = this.ppConfig.getFileTable();
    }
    
    private int getFileID(Path filePath) throws IOException {
        int fileID = this.fileTable.getSampleID(filePath.toString());
        if(fileID < 0) {
            throw new IOException(String.format("Cannot find fileID from path %s", filePath.toString()));
        }
        return fileID;
    }
    
    @Override
    protected void map(LongWritable key, ReadInfo value, Context context) throws IOException, InterruptedException {
        FileSplit fis = (FileSplit)context.getInputSplit();
        int fileID = getFileID(fis.getPath());
        
        String sequence = value.getSequence();
        if(sequence.length() >= this.kmerSize) {
            sequence = sequence.toUpperCase();
            boolean pvalid = false;
            for (int i = 0; i < (sequence.length() - this.kmerSize + 1); i++) {
                String kmer = sequence.substring(i, i + this.kmerSize);
                if (pvalid) {
                    if (!SequenceHelper.isValidSequence(kmer.charAt(this.kmerSize - 1))) {
                        //LOG.info("discard invalid kmer sequence : " + kmer);
                        pvalid = false;
                        continue;
                    } else {
                        pvalid = true;
                    }
                } else {
                    if (!SequenceHelper.isValidSequence(kmer)) {
                        //LOG.info("discard invalid kmer sequence : " + kmer);
                        pvalid = false;
                        continue;
                    } else {
                        pvalid = true;
                    }
                }
                
                String canonicalKmer = SequenceHelper.canonicalize(kmer);

                int[] arr = new int[2];
                arr[0] = fileID;
                arr[1] = 1;
                context.write(new CompressedSequenceWritable(canonicalKmer), new IntArrayWritable(arr));
            }
        }
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}
