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
package libra.common.kmermatch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import libra.common.hadoop.io.datatypes.CompressedSequenceWritable;
import libra.preprocess.common.helpers.KmerHistogramHelper;
import libra.preprocess.common.helpers.KmerIndexHelper;
import libra.preprocess.common.kmerhistogram.KmerHistogram;
import libra.preprocess.common.kmerhistogram.KmerHistogramRecord;
import libra.preprocess.common.kmerhistogram.KmerHistogramRecordComparator;
import libra.preprocess.common.kmerhistogram.KmerRangePartition;
import libra.preprocess.common.kmerhistogram.KmerRangePartitioner;
import libra.preprocess.common.kmerindex.KmerIndexIndexPathFilter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;

/**
 *
 * @author iychoi
 */
public class KmerMatchInputFormat extends SequenceFileInputFormat<CompressedSequenceWritable, KmerMatchResult> {

    private static final Log LOG = LogFactory.getLog(KmerMatchInputFormat.class);

    private final static String NUM_INPUT_FILES = "mapreduce.input.num.files";
    
    @Override
    public RecordReader<CompressedSequenceWritable, KmerMatchResult> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new KmerMatchRecordReader();
    }
    
    public static void setInputFormatConfig(JobContext job, KmerMatchInputFormatConfig inputFormatConfig) throws IOException {
        inputFormatConfig.saveTo(job.getConfiguration());
    }
    
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        KmerMatchInputFormatConfig inputFormatConfig = KmerMatchInputFormatConfig.createInstance(job.getConfiguration());
        
        // generate splits
        List<InputSplit> splits = new ArrayList<InputSplit>();
        List<FileStatus> files = listStatus(job);
        List<Path> kmerIndexFiles = new ArrayList<Path>();
        for (FileStatus file : files) {
            Path path = file.getPath();
            kmerIndexFiles.add(path);
        }
        
        LOG.info("# of Split input file : " + kmerIndexFiles.size());
        for(int i=0;i<kmerIndexFiles.size();i++) {
            LOG.info("> " + kmerIndexFiles.get(i).toString());
        }
        
        Path[] kmerIndexFilePath = kmerIndexFiles.toArray(new Path[0]);

        // histogram
        List<KmerHistogram> histograms = new ArrayList<KmerHistogram>();
        for(int i=0;i<kmerIndexFiles.size();i++) {
            String fastaFileName = KmerIndexHelper.getFastaFileName(kmerIndexFilePath[i]);
            
            Path histogramPath = new Path(inputFormatConfig.getKmerHistogramPath(), KmerHistogramHelper.makeKmerHistogramFileName(fastaFileName));
            FileSystem fs = histogramPath.getFileSystem(job.getConfiguration());
            if (fs.exists(histogramPath)) {
                KmerHistogram histogram = KmerHistogram.createInstance(fs, histogramPath);
                histograms.add(histogram);
            } else {
                throw new IOException("k-mer histogram is not found in given paths");
            }
        }
        
        // merge histogram
        Hashtable<String, KmerHistogramRecord> histogramRecords = new Hashtable<String, KmerHistogramRecord>();
        long kmerCounts = 0;
        for(int i=0;i<histograms.size();i++) {
            Collection<KmerHistogramRecord> records = histograms.get(i).getSortedRecord();
            kmerCounts += histograms.get(i).getTotalKmerCount();
            
            for(KmerHistogramRecord rec : records) {
                KmerHistogramRecord ext_rec = histogramRecords.get(rec.getKmer());
                if(ext_rec == null) {
                    histogramRecords.put(rec.getKmer(), rec);
                } else {
                    ext_rec.increaseFrequency(rec.getFrequency());
                }
            }
        }
        
        List<KmerHistogramRecord> histogramRecordsArr = new ArrayList<KmerHistogramRecord>();
        histogramRecordsArr.addAll(histogramRecords.values());
        Collections.sort(histogramRecordsArr, new KmerHistogramRecordComparator());
        
        KmerRangePartitioner partitioner = new KmerRangePartitioner(inputFormatConfig.getKmerSize(), inputFormatConfig.getPartitionNum());
        KmerRangePartition[] partitions = partitioner.getHistogramPartitions(histogramRecordsArr.toArray(new KmerHistogramRecord[0]), kmerCounts);
        
        for(KmerRangePartition partition : partitions) {
            splits.add(new KmerMatchInputSplit(kmerIndexFilePath, partition));
        }
        
        // Save the number of input files in the job-conf
        job.getConfiguration().setLong(NUM_INPUT_FILES, files.size());

        LOG.debug("Total # of splits: " + splits.size());
        return splits;
    }
    
    @Override
    protected List<FileStatus> listStatus(JobContext job) throws IOException {
        List<FileStatus> result = new ArrayList<FileStatus>();
        Path[] dirs = getInputPaths(job);
        if (dirs.length == 0) {
            throw new IOException("No input paths specified in job");
        }

        // get tokens for all the required FileSystems..
        TokenCache.obtainTokensForNamenodes(job.getCredentials(), dirs, job.getConfiguration());

        // creates a MultiPathFilter with the hiddenFileFilter and the
        // user provided one (if any).
        List<PathFilter> filters = new ArrayList<PathFilter>();
        PathFilter jobFilter = getInputPathFilter(job);
        if (jobFilter != null) {
            filters.add(jobFilter);
        }
        filters.add(new KmerIndexIndexPathFilter());
        PathFilter inputFilter = new MultiPathFilter(filters);

        for (int i = 0; i < dirs.length; ++i) {
            Path p = dirs[i];
            if(inputFilter.accept(p)) {
                FileSystem fs = p.getFileSystem(job.getConfiguration());
                FileStatus status = fs.getFileStatus(p);
                result.add(status);
            }
        }

        LOG.info("Total input paths to process : " + result.size());
        return result;
    }
    
    private static class MultiPathFilter implements PathFilter {

        private List<PathFilter> filters;

        public MultiPathFilter(List<PathFilter> filters) {
            this.filters = filters;
        }

        public boolean accept(Path path) {
            for (PathFilter filter : filters) {
                if (!filter.accept(path)) {
                    return false;
                }
            }
            return true;
        }
    }
}
