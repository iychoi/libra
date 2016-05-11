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
package libra.preprocess.stage2;

import libra.preprocess.common.helpers.KmerIndexHelper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import libra.common.hadoop.io.datatypes.CompressedSequenceWritable;
import libra.common.hadoop.io.format.fasta.FastaReadInputFormat;
import libra.common.helpers.FileSystemHelper;
import libra.common.report.Report;
import libra.common.cmdargs.CommandArgumentsParser;
import libra.common.helpers.MapReduceClusterHelper;
import libra.common.helpers.MapReduceHelper;
import libra.preprocess.PreprocessorCmdArgs;
import libra.preprocess.common.PreprocessorConfig;
import libra.preprocess.common.PreprocessorConfigException;
import libra.preprocess.common.helpers.KmerHistogramHelper;
import libra.preprocess.common.helpers.KmerStatisticsHelper;
import libra.preprocess.common.kmerindex.KmerIndexIndex;
import libra.preprocess.common.kmerstatistics.KmerStatistics;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author iychoi
 */
public class KmerIndexBuilder extends Configured implements Tool {
    
    private static final Log LOG = LogFactory.getLog(KmerIndexBuilder.class);
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new KmerIndexBuilder(), args);
        System.exit(res);
    }
    
    public static int main2(String[] args) throws Exception {
        return ToolRunner.run(new Configuration(), new KmerIndexBuilder(), args);
    }
    
    public KmerIndexBuilder() {
        
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
        
        if(ppConfig.getKmerIndexPath() == null) {
            throw new PreprocessorConfigException("cannot find kmer index path");
        }
    }
    
    private int runJob(PreprocessorConfig ppConfig) throws Exception {
        // check config
        validatePreprocessorConfig(ppConfig);
        
        // configuration
        Configuration conf = this.getConf();
        
        // set user configuration
        ppConfig.saveTo(conf);
        
        Path[] inputFiles = FileSystemHelper.getAllFastaFilePath(conf, ppConfig.getFastaPath());
        
        boolean job_result = true;
        List<Job> jobs = new ArrayList<Job>();
        
        for(int round=0;round<inputFiles.length;round++) {
            Path roundInputFile = inputFiles[round];
            String roundOutputPath = ppConfig.getKmerIndexPath() + "_round" + round;
            
            Job job = new Job(conf, "Libra Preprocessor - Building Kmer Indexes (" + round + " of " + inputFiles.length + ")");
            job.setJarByClass(KmerIndexBuilder.class);

            // Mapper
            job.setMapperClass(KmerIndexBuilderMapper.class);
            job.setInputFormatClass(FastaReadInputFormat.class);
            job.setMapOutputKeyClass(CompressedSequenceWritable.class);
            job.setMapOutputValueClass(IntWritable.class);
            
            // Combiner
            job.setCombinerClass(KmerIndexBuilderCombiner.class);
            
            // Partitioner
            job.setPartitionerClass(KmerIndexBuilderPartitioner.class);
            
            // Reducer
            job.setReducerClass(KmerIndexBuilderReducer.class);

            // Specify key / value
            job.setOutputKeyClass(CompressedSequenceWritable.class);
            job.setOutputValueClass(IntWritable.class);

            // Inputs
            FileInputFormat.addInputPaths(job, roundInputFile.toString());

            LOG.info("Input file : ");
            LOG.info("> " + roundInputFile.toString());
            
            String histogramFileName = KmerHistogramHelper.makeKmerHistogramFileName(roundInputFile.getName());
            Path histogramPath = new Path(ppConfig.getKmerHistogramPath(), histogramFileName);
            
            KmerIndexBuilderPartitioner.setHistogramPath(job.getConfiguration(), histogramPath);
            
            FileOutputFormat.setOutputPath(job, new Path(roundOutputPath));
            job.setOutputFormatClass(MapFileOutputFormat.class);
            
            // Use many reducers
            int reducers = conf.getInt("mapred.reduce.tasks", 0);
            if(reducers <= 0) {
                int MRNodes = MapReduceClusterHelper.getNodeNum(conf);
                reducers = MRNodes * 2;
                job.setNumReduceTasks(reducers);
            }
            LOG.info("Reducers : " + reducers);
            
            // Execute job and return status
            boolean result = job.waitForCompletion(true);
            
            jobs.add(job);

            // commit results
            if (result) {
                commitRoundIndexOutputFiles(roundInputFile, new Path(roundOutputPath), new Path(ppConfig.getKmerIndexPath()), job.getConfiguration(), ppConfig.getKmerSize());
                
                // create index of index
                createIndexOfIndex(new Path(ppConfig.getKmerIndexPath()), roundInputFile, job.getConfiguration(), ppConfig.getKmerSize());

                // create statistics of index
                createStatisticsOfIndex(new Path(ppConfig.getKmerStatisticsPath()), roundInputFile, job.getConfiguration(), job.getCounters(), ppConfig.getKmerSize());
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
    
    private void commitRoundIndexOutputFiles(Path roundInputPath, Path MROutputPath, Path finalOutputPath, Configuration conf, int kmerSize) throws IOException {
        FileSystem fs = MROutputPath.getFileSystem(conf);
        if(!fs.exists(finalOutputPath)) {
            fs.mkdirs(finalOutputPath);
        }
        
        FileStatus status = fs.getFileStatus(MROutputPath);
        if (status.isDir()) {
            FileStatus[] entries = fs.listStatus(MROutputPath);
            for (FileStatus entry : entries) {
                Path entryPath = entry.getPath();
                
                // remove unnecessary outputs
                if(MapReduceHelper.isLogFiles(entryPath)) {
                    fs.delete(entryPath, true);
                } else if(MapReduceHelper.isPartialOutputFiles(entryPath)) {
                    // rename outputs
                    int mapreduceID = MapReduceHelper.getMapReduceID(entryPath);
                    Path toPath = new Path(finalOutputPath, KmerIndexHelper.makeKmerIndexPartFileName(roundInputPath.getName(), kmerSize, mapreduceID));

                    LOG.info("output : " + entryPath.toString());
                    LOG.info("renamed to : " + toPath.toString());
                    fs.rename(entryPath, toPath);
                }
            }
        } else {
            throw new IOException("path not found : " + MROutputPath.toString());
        }
        
        fs.delete(MROutputPath, true);
    }
    
    private void createIndexOfIndex(Path indexPath, Path inputPath, Configuration conf, int kmerSize) throws IOException {
        String kmerIndexIndexFileName = KmerIndexHelper.makeKmerIndexIndexFileName(inputPath, kmerSize);
        Path kmerIndexIndexFilePath = new Path(indexPath, kmerIndexIndexFileName);
        Path[] indexFiles = KmerIndexHelper.getKmerIndexPartFilePath(conf, kmerIndexIndexFilePath);
        
        KmerIndexIndex indexIndex = new KmerIndexIndex();
        for(Path indexFile : indexFiles) {
            LOG.info("Reading the final key from " + indexFile.toString());
            MapFile.Reader reader = new MapFile.Reader(indexFile.getFileSystem(conf), indexFile.toString(), conf);
            CompressedSequenceWritable finalKey = new CompressedSequenceWritable();
            reader.finalKey(finalKey);
            indexIndex.addLastKey(finalKey.getSequence());
            reader.close();
        }

        LOG.info("Creating an index file : " + kmerIndexIndexFilePath.toString());
        indexIndex.saveTo(kmerIndexIndexFilePath.getFileSystem(conf), kmerIndexIndexFilePath);
    }

    private void createStatisticsOfIndex(Path statisticsPath, Path inputPath, Configuration conf, Counters counters, int kmerSize) throws IOException {
        CounterGroup logTFSquareGroup = counters.getGroup(KmerStatisticsHelper.getCounterGroupNameLogTFSquare());

        Iterator<Counter> logTFSquareGroupIterator = logTFSquareGroup.iterator();
        while(logTFSquareGroupIterator.hasNext()) {
            Counter logTFSquareCounter = logTFSquareGroupIterator.next();
            if(logTFSquareCounter.getName().equals(inputPath.getName())) {
                double logTFSquare = 0;
                double tf_cosnorm_base = 0;

                logTFSquare = logTFSquareCounter.getValue() / 1000.0;

                tf_cosnorm_base = Math.sqrt(logTFSquare);
                LOG.info("tf-cos-norm-base " + logTFSquareCounter.getName() + " : " + tf_cosnorm_base);

                Path outputHadoopPath = new Path(statisticsPath, KmerStatisticsHelper.makeKmerStatisticsFileName(logTFSquareCounter.getName()));
                FileSystem fs = outputHadoopPath.getFileSystem(conf);

                KmerStatistics statistics = new KmerStatistics();
                statistics.setSampleName(logTFSquareCounter.getName());
                statistics.setKmerSize(kmerSize);
                statistics.setTFCosineNormBase(tf_cosnorm_base);

                statistics.saveTo(fs, outputHadoopPath);
            }
        }
    }
}
