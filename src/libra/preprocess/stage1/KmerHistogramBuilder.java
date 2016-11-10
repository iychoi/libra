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
import libra.common.hadoop.io.format.fasta.FastaKmerInputFormat;
import libra.common.helpers.FileSystemHelper;
import libra.common.report.Report;
import libra.common.cmdargs.CommandArgumentsParser;
import libra.common.helpers.MapReduceHelper;
import libra.preprocess.PreprocessorCmdArgs;
import libra.preprocess.common.PreprocessorConfig;
import libra.preprocess.common.PreprocessorConfigException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author iychoi
 */
@SuppressWarnings("deprecation")
public class KmerHistogramBuilder extends Configured implements Tool {
    
    private static final Log LOG = LogFactory.getLog(KmerHistogramBuilder.class);
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new KmerHistogramBuilder(), args);
        System.exit(res);
    }
    
    public static int main2(String[] args) throws Exception {
        return ToolRunner.run(new Configuration(), new KmerHistogramBuilder(), args);
    }
    
    public KmerHistogramBuilder() {
        
    }
    
    @Override
    public int run(String[] args) throws Exception {
        CommandArgumentsParser<PreprocessorCmdArgs> parser = new CommandArgumentsParser<PreprocessorCmdArgs>();
        PreprocessorCmdArgs cmdParams = new PreprocessorCmdArgs();
        if(!parser.parse(args, cmdParams)) {
            LOG.error("Failed to parse command line arguments!");
            return 1;
        }
        
        PreprocessorConfig ppConfig = cmdParams.getPreprocessorConfig();
        
        return runJob(ppConfig);
    }
    
    private void validatePreprocessorConfig(PreprocessorConfig ppConfig) throws PreprocessorConfigException {
        if(ppConfig.getFastaPath().size() <= 0) {
            throw new PreprocessorConfigException("cannot find input sample path");
        }
        
        if(ppConfig.getKmerHistogramPath() == null) {
            throw new PreprocessorConfigException("cannot find kmer histogram path");
        }
        
        if(ppConfig.getKmerSize() <= 0) {
            throw new PreprocessorConfigException("invalid kmer size");
        }
    }
    
    private int runJob(PreprocessorConfig ppConfig) throws Exception {
        // check config
        validatePreprocessorConfig(ppConfig);
        
        // configuration
        Configuration conf = this.getConf();
        
        Job job = new Job(conf, "Libra Preprocessor - Building Kmer Histogram");
        conf = job.getConfiguration();
        
        // set user configuration
        ppConfig.saveTo(conf);
        
        job.setJarByClass(KmerHistogramBuilder.class);

        // Mapper
        job.setMapperClass(KmerHistogramBuilderMapper.class);
        FastaKmerInputFormat.setSplitable(conf, false);
        FastaKmerInputFormat.setKmerSize(conf, ppConfig.getKmerSize());
        job.setInputFormatClass(FastaKmerInputFormat.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        
        // Specify key / value
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        
        // Inputs
        Path[] inputFiles = FileSystemHelper.getAllFastaFilePath(conf, ppConfig.getFastaPath());
        FileInputFormat.addInputPaths(job, FileSystemHelper.makeCommaSeparated(inputFiles));
        
        LOG.info("Input sample files : " + inputFiles.length);
        for(Path inputFile : inputFiles) {
            LOG.info("> " + inputFile.toString());
        }
        
        job.setOutputFormatClass(NullOutputFormat.class);
        
        // Map only job
        job.setNumReduceTasks(0);
        
        // Execute job and return status
        boolean result = job.waitForCompletion(true);

        // commit results
        if(result) {
            commit(new Path(ppConfig.getKmerHistogramPath()), conf);
        }
        
        // report
        if(ppConfig.getReportPath() != null && !ppConfig.getReportPath().isEmpty()) {
            Report report = new Report();
            report.addJob(job);
            report.writeTo(ppConfig.getReportPath());
        }
        
        return result ? 0 : 1;
    }

    private void commit(Path outputPath, Configuration conf) throws IOException {
        FileSystem fs = outputPath.getFileSystem(conf);
        
        FileStatus status = fs.getFileStatus(outputPath);
        if (status.isDir()) {
            FileStatus[] entries = fs.listStatus(outputPath);
            for (FileStatus entry : entries) {
                Path entryPath = entry.getPath();
                
                // remove unnecessary outputs
                if(MapReduceHelper.isLogFiles(entryPath)) {
                    fs.delete(entryPath, true);
                } else if(MapReduceHelper.isPartialOutputFiles(entryPath)) {
                    fs.delete(entryPath, true);
                } else if(KmerHistogramHelper.isKmerHistogramFile(entryPath)) {
                    // not necessary
                }
            }
        } else {
            throw new IOException("path not found : " + outputPath.toString());
        }
    }
}
