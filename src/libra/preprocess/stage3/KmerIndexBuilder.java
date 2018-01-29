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
package libra.preprocess.stage3;

import libra.preprocess.common.helpers.KmerIndexHelper;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import libra.common.hadoop.io.datatypes.IntArrayWritable;
import libra.common.hadoop.io.datatypes.CompressedSequenceWritable;
import libra.common.helpers.FileSystemHelper;
import libra.common.report.Report;
import libra.common.hadoop.io.format.sequence.SequenceKmerInputFormat;
import libra.common.helpers.MapReduceHelper;
import libra.preprocess.common.FilterAlgorithm;
import libra.preprocess.common.PreprocessorConfigException;
import libra.preprocess.common.PreprocessorRoundConfig;
import libra.preprocess.common.filetable.FileTable;
import libra.preprocess.common.helpers.KmerFilterHelper;
import libra.preprocess.common.helpers.KmerHistogramHelper;
import libra.preprocess.common.helpers.KmerStatisticsHelper;
import libra.preprocess.common.kmerfilter.KmerFilterTable;
import libra.preprocess.common.kmerindex.KmerIndexTable;
import libra.preprocess.common.kmerindex.KmerIndexTableRecord;
import libra.preprocess.common.kmerstatistics.KmerStatistics;
import libra.preprocess.common.kmerstatistics.KmerStatisticsPart;
import libra.preprocess.common.kmerstatistics.KmerStatisticsPartTable;
import libra.preprocess.common.kmerstatistics.KmerStatisticsTable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;

/**
 *
 * @author iychoi
 */
public class KmerIndexBuilder {
    
    private static final Log LOG = LogFactory.getLog(KmerIndexBuilder.class);
    
    public KmerIndexBuilder() {
        
    }
    
    private void validatePreprocessorConfig(PreprocessorRoundConfig ppConfig) throws PreprocessorConfigException {
        if(ppConfig.getSequencePath().size() <= 0) {
            throw new PreprocessorConfigException("cannot find input sample path");
        }
        
        if(ppConfig.getFileTable() == null || ppConfig.getFileTable().samples() <= 0) {
            throw new PreprocessorConfigException("cannot find input sample path");
        }
        
        if(ppConfig.getUseHistogram()) {
            if(ppConfig.getKmerHistogramPath() == null) {
                throw new PreprocessorConfigException("cannot find kmer histogram path");
            }
        }
        
        if(ppConfig.getKmerSize() <= 0) {
            throw new PreprocessorConfigException("invalid kmer size");
        }
        
        if(ppConfig.getKmerIndexPath() == null) {
            throw new PreprocessorConfigException("cannot find kmer index path");
        }
    }
    
    public int runJob(Configuration conf, PreprocessorRoundConfig ppConfig) throws Exception {
        // check config
        validatePreprocessorConfig(ppConfig);
        
        Job job = Job.getInstance(conf, "Libra Preprocessor - Building Kmer Index");
        conf = job.getConfiguration();

        // set user configuration
        ppConfig.saveTo(conf);
        
        Report report = new Report();
        
        job.setJarByClass(KmerIndexBuilder.class);
        
        // Mapper
        job.setMapperClass(KmerIndexBuilderMapper.class);
        SequenceKmerInputFormat.setKmerSize(conf, ppConfig.getKmerSize());
        job.setInputFormatClass(SequenceKmerInputFormat.class);
        job.setMapOutputKeyClass(CompressedSequenceWritable.class);
        job.setMapOutputValueClass(IntArrayWritable.class);
        
        // Combiner
        job.setCombinerClass(KmerIndexBuilderCombiner.class);

        // Partitioner
        job.setPartitionerClass(KmerIndexBuilderPartitioner.class);

        // Reducer
        job.setReducerClass(KmerIndexBuilderReducer.class);
        
        // Specify key / value
        job.setOutputKeyClass(CompressedSequenceWritable.class);
        job.setOutputValueClass(IntArrayWritable.class);
        
        // Inputs
        Path[] inputFiles = FileSystemHelper.makePathFromString(conf, ppConfig.getFileTable().getSamples());
        FileInputFormat.addInputPaths(job, FileSystemHelper.makeCommaSeparated(inputFiles));
        
        LOG.info("Input sample files : " + inputFiles.length);
        for(Path inputFile : inputFiles) {
            LOG.info("> " + inputFile.toString());
        }
        
        // filter
        if(ppConfig.getFilterAlgorithm() != FilterAlgorithm.NONE) {
            // read filter
            String filterTableFileName = KmerFilterHelper.makeKmerFilterTableFileName(ppConfig.getFileTable().getName());
            Path filterTablePath = new Path(ppConfig.getKmerFilterPath(), filterTableFileName);
            
            FileSystem fs = filterTablePath.getFileSystem(conf);
            KmerFilterTable filterTable = KmerFilterTable.createInstance(fs, filterTablePath);
            
            filterTable.saveTo(conf);
        }
        
        // histogram
        if(ppConfig.getUseHistogram()) {
            String histogramFileName = KmerHistogramHelper.makeKmerHistogramFileName(ppConfig.getFileTable().getName());
            Path histogramPath = new Path(ppConfig.getKmerHistogramPath(), histogramFileName);

            KmerIndexBuilderPartitioner.setHistogramPath(conf, histogramPath);
        }

        // output
        String tempKmerIndexPath = ppConfig.getKmerIndexPath() + "_temp";
        FileOutputFormat.setOutputPath(job, new Path(tempKmerIndexPath));
        job.setOutputFormatClass(MapFileOutputFormat.class);

        // reducers
        int reducers = conf.getInt("mapred.reduce.tasks", 1);
        if(ppConfig.getTaskNum() > 0) {
            reducers = ppConfig.getTaskNum();
        }
        
        job.setNumReduceTasks(reducers);
        LOG.info("# of Reducers : " + reducers);
        
        // Execute job and return status
        boolean result = job.waitForCompletion(true);
        
        // commit results
        if(result) {
            commit(ppConfig.getFileTable(), new Path(tempKmerIndexPath), new Path(ppConfig.getKmerIndexPath()), conf);
            
            // create index of index
            createIndexTable(new Path(ppConfig.getKmerIndexPath()), ppConfig.getFileTable(), conf);
            
            // create statistics of index
            createStatistics(new Path(ppConfig.getKmerStatisticsPath()), ppConfig.getFileTable(), conf);
        }
        
        report.addJob(job);
        
        // report
        if(ppConfig.getReportPath() != null && !ppConfig.getReportPath().isEmpty()) {
            report.writeTo(ppConfig.getReportPath());
        }
        
        return result ? 0 : 1;
    }
    
    private void commit(FileTable fileTable, Path MROutputPath, Path finalOutputPath, Configuration conf) throws IOException {
        FileSystem fs = MROutputPath.getFileSystem(conf);
        if(!fs.exists(finalOutputPath)) {
            fs.mkdirs(finalOutputPath);
        }
        
        FileStatus status = fs.getFileStatus(MROutputPath);
        if (status.isDirectory()) {
            FileStatus[] entries = fs.listStatus(MROutputPath);
            for (FileStatus entry : entries) {
                Path entryPath = entry.getPath();
                
                // remove unnecessary outputs
                if(MapReduceHelper.isLogFiles(entryPath)) {
                    fs.delete(entryPath, true);
                } else if(MapReduceHelper.isPartialOutputFiles(entryPath)) {
                    // rename outputs
                    int mapreduceID = MapReduceHelper.getMapReduceID(entryPath);
                    Path toPath = new Path(finalOutputPath, KmerIndexHelper.makeKmerIndexDataFileName(fileTable.getName(), mapreduceID));

                    LOG.info(String.format("rename %s ==> %s", entryPath.toString(), toPath.toString()));
                    fs.rename(entryPath, toPath);
                }
            }
        } else {
            throw new IOException("path not found : " + MROutputPath.toString());
        }
        
        fs.delete(MROutputPath, true);
    }
    
    private void createIndexTable(Path indexPath, FileTable fileTable, Configuration conf) throws IOException {
        String kmerIndexTableFileName = KmerIndexHelper.makeKmerIndexTableFileName(fileTable.getName());
        Path kmerIndexTableFilePath = new Path(indexPath, kmerIndexTableFileName);
        
        LOG.info("Creating an index file : " + kmerIndexTableFilePath.toString());
        
        Path[] indexDataFiles = KmerIndexHelper.getKmerIndexDataFilePath(conf, indexPath);
        KmerIndexTable indexTable = new KmerIndexTable(fileTable.getName());
        for(Path indexDataFile : indexDataFiles) {
            if(KmerIndexHelper.isSameKmerIndex(kmerIndexTableFilePath, indexDataFile)) {
                MapFile.Reader reader = new MapFile.Reader(indexDataFile, conf, new SequenceFile.Reader.Option[0]);
                CompressedSequenceWritable finalKey = new CompressedSequenceWritable();
                reader.finalKey(finalKey);
                if(!finalKey.isEmpty()) {
                    indexTable.addRecord(new KmerIndexTableRecord(indexDataFile.getName(), finalKey.getSequence()));
                    reader.close();
                }
            }
        }
        
        indexTable.saveTo(kmerIndexTableFilePath.getFileSystem(conf), kmerIndexTableFilePath);
    }

    private void createStatistics(Path statisticsPath, FileTable fileTable, Configuration conf) throws IOException {
        String statisticsTableFileName = KmerStatisticsHelper.makeKmerStatisticsTableFileName(fileTable.getName());
        Path statisticsTableOutputFile = new Path(statisticsPath, statisticsTableFileName);
        
        LOG.info("Creating a statistics table file : " + statisticsTableFileName);
        
        Collection<String> samples = fileTable.getSamples();
        
        KmerStatisticsPart[] statisticsWeight = new KmerStatisticsPart[fileTable.samples()];
        KmerStatistics[] statistics = new KmerStatistics[fileTable.samples()];
        Iterator<String> iterator = samples.iterator();
        for(int i=0;i<fileTable.samples();i++) {
            String sample_name = iterator.next();
            statisticsWeight[i] = new KmerStatisticsPart(sample_name);
            statistics[i] = new KmerStatistics(sample_name);
        }
        
        Path[] kmerStatisticsTablePartFiles = KmerStatisticsHelper.getKmerStatisticsPartTableFilePath(conf, statisticsPath);
        for(Path statisticsTablePartFile : kmerStatisticsTablePartFiles) {
            FileSystem fs = statisticsTablePartFile.getFileSystem(conf);
            KmerStatisticsPartTable table = KmerStatisticsPartTable.createInstance(fs, statisticsTablePartFile);
            int idx = 0;
            for(KmerStatisticsPart statisticsPart : table.getStatisticsPart()) {
                statisticsWeight[idx].incrementBooleanTFWeight(statisticsPart.getBooleanTFWeight());
                statisticsWeight[idx].incrementLogTFWeight(statisticsPart.getLogTFWeight());
                statisticsWeight[idx].incrementNaturalTFWeight(statisticsPart.getNaturalTFWeight());
                idx++;
            }
        }
        
        
        KmerStatisticsTable statisticsTable = new KmerStatisticsTable();
        statisticsTable.setName(fileTable.getName());
        
        for(int j=0;j<fileTable.samples();j++) {
            double bool_norm_base = Math.sqrt(statisticsWeight[j].getBooleanTFWeight());
            statistics[j].setBooleanTFCosineNormBase(bool_norm_base);
            
            double log_norm_base = Math.sqrt(statisticsWeight[j].getLogTFWeight());
            statistics[j].setLogTFCosineNormBase(log_norm_base);
            
            double natural_norm_base = Math.sqrt(statisticsWeight[j].getNaturalTFWeight());
            statistics[j].setNaturalTFCosineNormBase(natural_norm_base);
            
            statisticsTable.addStatistics(statistics[j]);
        }
        
        // save
        FileSystem outputFileSystem = statisticsTableOutputFile.getFileSystem(conf);

        statisticsTable.saveTo(outputFileSystem, statisticsTableOutputFile);
        
        // delete part files
        for(Path statisticsTablePartFile : kmerStatisticsTablePartFiles) {
            FileSystem fs = statisticsTablePartFile.getFileSystem(conf);
            fs.delete(statisticsTablePartFile, true);
        }
    }
}
