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
import java.util.Collection;
import java.util.Iterator;
import libra.common.hadoop.io.datatypes.IntArrayWritable;
import libra.common.hadoop.io.datatypes.CompressedSequenceWritable;
import libra.common.helpers.FileSystemHelper;
import libra.common.report.Report;
import libra.common.hadoop.io.format.sequence.SequenceFileInputFormat;
import libra.preprocess.common.PreprocessorConfigException;
import libra.preprocess.common.PreprocessorRoundConfig;
import libra.preprocess.common.filetable.FileTable;
import libra.preprocess.common.helpers.KmerFilterHelper;
import libra.preprocess.common.kmerfilter.KmerFilter;
import libra.preprocess.common.kmerfilter.KmerFilterPart;
import libra.preprocess.common.kmerfilter.KmerFilterPartTable;
import libra.preprocess.common.kmerfilter.KmerFilterTable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

/**
 *
 * @author iychoi
 */
public class KmerFilterBuilder {
    
    private static final Log LOG = LogFactory.getLog(KmerFilterBuilder.class);
    
    public KmerFilterBuilder() {
        
    }
    
    private void validatePreprocessorConfig(PreprocessorRoundConfig ppConfig) throws PreprocessorConfigException {
        if(ppConfig.getSamplePath().size() <= 0) {
            throw new PreprocessorConfigException("cannot find input sample path");
        }
        
        if(ppConfig.getFileTable() == null || ppConfig.getFileTable().samples() <= 0) {
            throw new PreprocessorConfigException("cannot find input sample path");
        }
        
        if(ppConfig.getKmerSize() <= 0) {
            throw new PreprocessorConfigException("invalid kmer size");
        }
        
        if(ppConfig.getKmerFilterPath()== null) {
            throw new PreprocessorConfigException("cannot find kmer filter path");
        }
    }
    
    public int runJob(Configuration conf, PreprocessorRoundConfig ppConfig) throws Exception {
        // check config
        validatePreprocessorConfig(ppConfig);
        
        Job job = Job.getInstance(conf, "Libra - Building Kmer Filter");
        conf = job.getConfiguration();

        // set user configuration
        ppConfig.saveTo(conf);
        
        Report report = new Report();
        
        job.setJarByClass(KmerFilterBuilder.class);
        
        // Mapper
        job.setMapperClass(KmerFilterBuilderMapper.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapOutputKeyClass(CompressedSequenceWritable.class);
        job.setMapOutputValueClass(IntArrayWritable.class);
        
        // Combiner
        job.setCombinerClass(KmerFilterBuilderCombiner.class);

        // Partitioner
        job.setPartitionerClass(KmerFilterBuilderPartitioner.class);

        // Reducer
        job.setReducerClass(KmerFilterBuilderReducer.class);
        
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
            commit(new Path(ppConfig.getKmerFilterPath()), ppConfig.getFileTable(), conf);
        }
        
        report.addJob(job);
        
        // report
        if(ppConfig.getReportPath() != null && !ppConfig.getReportPath().isEmpty()) {
            report.writeTo(ppConfig.getReportPath());
        }
        
        return result ? 0 : 1;
    }
    
    private void commit(Path filterPath, FileTable fileTable, Configuration conf) throws IOException {
        String filterTableFileName = KmerFilterHelper.makeKmerFilterTableFileName(fileTable.getName());
        Path filterTableOutputFile = new Path(filterPath, filterTableFileName);
        
        LOG.info("Creating a kmer filter table file : " + filterTableFileName);
        
        Collection<String> samples = fileTable.getSamples();
        
        KmerFilterPart[] filterParts = new KmerFilterPart[fileTable.samples()];
        KmerFilter[] filter = new KmerFilter[fileTable.samples()];
        Iterator<String> iterator = samples.iterator();
        for(int i=0;i<fileTable.samples();i++) {
            String sample_name = iterator.next();
            filterParts[i] = new KmerFilterPart(sample_name);
            filter[i] = new KmerFilter(sample_name);
        }
        
        Path[] kmerFilterTablePartFiles = KmerFilterHelper.getKmerFilterPartTableFilePaths(conf, filterPath);
        for(Path filterTablePartFile : kmerFilterTablePartFiles) {
            FileSystem fs = filterTablePartFile.getFileSystem(conf);
            KmerFilterPartTable table = KmerFilterPartTable.createInstance(fs, filterTablePartFile);
            int idx = 0;
            for(KmerFilterPart filterPart : table.getFilterPart()) {
                filterParts[idx].incrementTotalKmers(filterPart.getTotalKmers());
                filterParts[idx].incrementUniqueKmers(filterPart.getUniqueKmers());
                filterParts[idx].incrementSumOfSquare(filterPart.getSumOfSquare());
                idx++;
            }
        }
        
        
        KmerFilterTable filterTable = new KmerFilterTable();
        filterTable.setName(fileTable.getName());
        
        for(int j=0;j<fileTable.samples();j++) {
            filter[j].setTotalKmers(filterParts[j].getTotalKmers());
            filter[j].setUniqueKmers(filterParts[j].getUniqueKmers());
            filter[j].setSumOfSquare(filterParts[j].getSumOfSquare());
            
            filterTable.addFilter(filter[j]);
        }
        
        // save
        FileSystem outputFileSystem = filterTableOutputFile.getFileSystem(conf);

        filterTable.saveTo(outputFileSystem, filterTableOutputFile);
        
        // delete part files
        for(Path filterTablePartFile : kmerFilterTablePartFiles) {
            FileSystem fs = filterTablePartFile.getFileSystem(conf);
            fs.delete(filterTablePartFile, true);
        }
    }
}
