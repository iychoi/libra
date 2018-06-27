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
package libra.common.hadoop.io.format.sequence;

import java.io.IOException;
import libra.common.hadoop.io.reader.sequence.ReadRecordReader;
import libra.common.sequence.ReadInfo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 *
 * @author iychoi
 */
public class SequenceFileInputFormat extends FileInputFormat<LongWritable, ReadInfo> {

    private static final Log LOG = LogFactory.getLog(SequenceFileInputFormat.class);
    
    @Override
    public RecordReader<LongWritable, ReadInfo> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new ReadRecordReader();
    }
    
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        final CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(filename);
        if(codec == null) {
            return true;
        }
        
        return codec instanceof SplittableCompressionCodec;
    }
}