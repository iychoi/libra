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

import libra.preprocess.common.helpers.KmerHistogramHelper;
import java.io.IOException;
import libra.common.hadoop.io.format.sequence.SequenceKmerInputFormat;
import libra.common.helpers.FileSystemHelper;
import libra.common.report.Report;
import libra.common.helpers.MapReduceHelper;
import libra.preprocess.common.PreprocessorConfigException;
import libra.preprocess.common.PreprocessorRoundConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

/**
 *
 * @author iychoi
 */
public class KmerHistogramBuilder {
    private static final Log LOG = LogFactory.getLog(KmerHistogramBuilder.class);
    
    public KmerHistogramBuilder() {
        
    }
    
    private void validatePreprocessorConfig(PreprocessorRoundConfig ppConfig) throws PreprocessorConfigException {
        if(ppConfig.getSequencePath().size() <= 0) {
            throw new PreprocessorConfigException("cannot find input sample path");
        }
        
        if(ppConfig.getFileTable() == null || ppConfig.getFileTable().samples() <= 0) {
            throw new PreprocessorConfigException("cannot find input sample path");
        }
        
        if(ppConfig.getKmerHistogramPath() == null) {
            throw new PreprocessorConfigException("cannot find kmer histogram path");
        }
        
        if(ppConfig.getKmerSize() <= 0) {
            throw new PreprocessorConfigException("invalid kmer size");
        }
    }
    
    public int runJob(Configuration conf, PreprocessorRoundConfig ppConfig) throws Exception {
        // check config
        validatePreprocessorConfig(ppConfig);
        
        Job job = Job.getInstance(conf, "Libra Preprocessor - Building Kmer Histogram");
        conf = job.getConfiguration();
        
        // set user configuration
        ppConfig.saveTo(conf);
        
        Report report = new Report();
        
        job.setJarByClass(KmerHistogramBuilder.class);

        // Mapper
        job.setMapperClass(KmerHistogramBuilderMapper.class);
        SequenceKmerInputFormat.setKmerSize(conf, ppConfig.getKmerSize());
        job.setInputFormatClass(SequenceKmerInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        
        // Combiner
        job.setCombinerClass(KmerHistogramBuilderCombiner.class);
        
        // Reducer
        job.setReducerClass(KmerHistogramBuilderReducer.class);
        
        // Specify key / value
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        
        // Inputs
        Path[] inputFiles = FileSystemHelper.makePathFromString(conf, ppConfig.getFileTable().getSamples());
        FileInputFormat.addInputPaths(job, FileSystemHelper.makeCommaSeparated(inputFiles));
        
        LOG.info("Input sample files : " + inputFiles.length);
        for(Path inputFile : inputFiles) {
            LOG.info("> " + inputFile.toString());
        }
        
        job.setOutputFormatClass(NullOutputFormat.class);
        
        job.setNumReduceTasks(1);
        
        // Execute job and return status
        boolean result = job.waitForCompletion(true);

        report.addJob(job);
        
        // report
        if(ppConfig.getReportPath() != null && !ppConfig.getReportPath().isEmpty()) {
            report.writeTo(ppConfig.getReportPath());
        }
        
        return result ? 0 : 1;
    }
}
