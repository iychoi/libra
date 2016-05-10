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
package libra.preprocess.indexing.stage3;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import libra.common.helpers.FileSystemHelper;
import libra.common.report.Report;
import libra.common.cmdargs.CommandArgumentsParser;
import libra.preprocess.PreprocessorCmdArgs;
import libra.preprocess.common.PreprocessorConfig;
import libra.preprocess.common.PreprocessorConfigException;
import libra.preprocess.common.helpers.KmerIndexHelper;
import libra.preprocess.common.helpers.KmerStatisticsHelper;
import libra.preprocess.common.kmerstatistics.KmerStatistics;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author iychoi
 */
public class KmerStatisticsBuilder extends Configured implements Tool {
    
    private static final Log LOG = LogFactory.getLog(KmerStatisticsBuilder.class);
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new KmerStatisticsBuilder(), args);
        System.exit(res);
    }
    
    public static int main2(String[] args) throws Exception {
        return ToolRunner.run(new Configuration(), new KmerStatisticsBuilder(), args);
    }
    
    public KmerStatisticsBuilder() {
        
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
        if(ppConfig.getKmerHistogramPath() == null) {
            throw new PreprocessorConfigException("cannot find kmer histogram path");
        }
        
        if(ppConfig.getKmerIndexPath() == null) {
            throw new PreprocessorConfigException("cannot find kmer index path");
        }
        
        if(ppConfig.getKmerStatisticsPath() == null) {
            throw new PreprocessorConfigException("cannot find kmer statistics path");
        }
    }
    
    private int runJob(PreprocessorConfig ppConfig) throws Exception {
        // check config
        validatePreprocessorConfig(ppConfig);
        
        // configuration
        Configuration conf = this.getConf();
        
        // set user configuration
        ppConfig.saveTo(conf);
        
        Path[] inputFiles = KmerIndexHelper.getAllKmerIndexIndexFilePath(conf, ppConfig.getKmerIndexPath());
        
        for(Path inputFile : inputFiles) {
            LOG.info(inputFile);
        }
        
        boolean job_result = true;
        List<Job> jobs = new ArrayList<Job>();
        
        for(int round=0;round<inputFiles.length;round++) {
            Path roundInputFile = inputFiles[round];
            Path[] roundInputKmerIndexPartFiles = KmerIndexHelper.getKmerIndexPartFilePath(conf, roundInputFile);
            
            Job job = new Job(conf, "Libra Preprocessor - Computing Kmer Statistics (" + round + " of " + inputFiles.length + ")");
            job.setJarByClass(KmerStatisticsBuilder.class);
            
            // Mapper
            job.setMapperClass(KmerStatisticsBuilderMapper.class);
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setMapOutputKeyClass(NullWritable.class);
            job.setMapOutputValueClass(NullWritable.class);

            // Specify key / value
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(NullWritable.class);

            // Inputs
            Path[] kmerIndexPartDataFiles = KmerIndexHelper.getAllKmerIndexPartDataFilePath(conf, roundInputKmerIndexPartFiles);
            SequenceFileInputFormat.addInputPaths(job, FileSystemHelper.makeCommaSeparated(kmerIndexPartDataFiles));

            LOG.info("Input file : ");
            LOG.info("> " + roundInputFile.toString());
            
            // Outputs
            job.setOutputFormatClass(NullOutputFormat.class);

            job.setNumReduceTasks(0);

            // Execute job and return status
            boolean result = job.waitForCompletion(true);

            jobs.add(job);

            // check results
            if(result) {
                CounterGroup logTFSquareGroup = job.getCounters().getGroup(KmerStatisticsHelper.getCounterGroupNameLogTFSquare());

                Iterator<Counter> logTFSquareGroupIterator = logTFSquareGroup.iterator();
                while(logTFSquareGroupIterator.hasNext()) {
                    double logTFSquare = 0;
                    double tf_cosnorm_base = 0;

                    Counter logTFSquareCounter = logTFSquareGroupIterator.next();

                    logTFSquare = logTFSquareCounter.getValue() / 1000.0;

                    tf_cosnorm_base = Math.sqrt(logTFSquare);
                    LOG.info("tf-cos-norm-base " + logTFSquareCounter.getName() + " : " + tf_cosnorm_base);

                    Path outputHadoopPath = new Path(ppConfig.getKmerStatisticsPath(), KmerStatisticsHelper.makeKmerStatisticsFileName(logTFSquareCounter.getName()));
                    FileSystem fs = outputHadoopPath.getFileSystem(conf);

                    KmerStatistics statistics = new KmerStatistics();
                    statistics.setSampleName(logTFSquareCounter.getName());
                    statistics.setKmerSize(ppConfig.getKmerSize());
                    statistics.setTFCosineNormBase(tf_cosnorm_base);

                    statistics.saveTo(fs, outputHadoopPath);
                }
            }
            
            if(!result) {
                LOG.error("job failed at round " + round + " of " + inputFiles.length);
                job_result = false;
                break;
            }
        }
        
        // report
        if(ppConfig.getReportPath() != null && !ppConfig.getReportPath().isEmpty()) {
            Report report = new Report();
            report.addJob(jobs);
            report.writeTo(ppConfig.getReportPath());
        }
        
        return job_result ? 0 : 1;
    }
}
