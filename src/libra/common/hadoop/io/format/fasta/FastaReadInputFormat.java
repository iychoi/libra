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
package libra.common.hadoop.io.format.fasta;

import java.io.IOException;
import libra.common.fasta.FastaRead;
import libra.common.hadoop.io.reader.fasta.FastaReadReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 *
 * @author iychoi
 */
public class FastaReadInputFormat extends FileInputFormat<LongWritable, FastaRead> {

    private static final Log LOG = LogFactory.getLog(FastaReadInputFormat.class);
    
    private final static String CONF_SPLITABLE = "libra.comm.hadoop.io.format.fasta.splitable";
    
    @Override
    public RecordReader<LongWritable, FastaRead> createRecordReader(InputSplit split,
            TaskAttemptContext context) throws IOException,
            InterruptedException {
        return new FastaReadReader();
    }
    
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        boolean splitable = FastaReadInputFormat.isSplitable(context.getConfiguration());
        LOG.info("splitable = " + splitable);
        if(!splitable) {
            return false;
        }
        
        CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(filename);
        if(codec != null) {
            return false;
        }
        
        return true;
    }
    
    public static void setSplitable(Configuration conf, boolean splitable) {
        conf.setBoolean(CONF_SPLITABLE, splitable);
    }
    
    public static boolean isSplitable(Configuration conf) {
        return conf.getBoolean(CONF_SPLITABLE, true);
    }
}