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


import java.io.IOException;
import java.util.Arrays;
import libra.common.hadoop.io.datatypes.IntArrayWritable;
import libra.common.hadoop.io.datatypes.CompressedSequenceWritable;
import libra.common.hadoop.io.datatypes.CompressedSequenceWritableComparator;
import libra.preprocess.common.PreprocessorRoundConfig;
import libra.preprocess.common.kmerhistogram.KmerHistogram;
import libra.preprocess.common.kmerhistogram.KmerRangePartition;
import libra.preprocess.common.kmerhistogram.KmerRangePartitioner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *
 * @author iychoi
 */
public class KmerIndexBuilderPartitioner extends Partitioner<CompressedSequenceWritable, IntArrayWritable> implements Configurable {

    private static final Log LOG = LogFactory.getLog(KmerIndexBuilderPartitioner.class);
    
    private final static String HISTOGRAM_FILE = "libra.kmerindex.histogram_file";
    
    private Configuration conf;
    
    private boolean initialized = false;
    private PreprocessorRoundConfig ppConfig;
    private KmerRangePartition[] partitions;
    private CompressedSequenceWritable[] partitionEndKeys;
    
    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }
    
    public static void setHistogramPath(Configuration config, Path histogramPath) throws IOException {
        config.set(HISTOGRAM_FILE, histogramPath.toString());
    }
    
    public static Path getHistogramPath(Configuration config) throws IOException {
        String histogramPath = config.get(HISTOGRAM_FILE);
        if(histogramPath == null) {
            return null;
        }
        return new Path(histogramPath);
    }
    
    private void initialize() throws IOException {
        this.ppConfig = PreprocessorRoundConfig.createInstance(this.conf);
        
        this.partitions = null;
        this.partitionEndKeys = null;
    }
    
    private void initializeSecond(int numReduceTasks) throws IOException {
        if(this.partitions == null) {
            if(this.ppConfig.getUseHistogram()) {
                KmerHistogram histogram = null;
                // search index file
                Path histogramPath = getHistogramPath(this.conf);
                FileSystem fs = histogramPath.getFileSystem(this.conf);
                if (fs.exists(histogramPath)) {
                    histogram = KmerHistogram.createInstance(fs, histogramPath);
                } else {
                    throw new IOException("k-mer histogram is not found in given paths");
                }

                LOG.info("Use variable range partitioning by histogram");
                KmerRangePartitioner partitioner = new KmerRangePartitioner(this.ppConfig.getKmerSize(), numReduceTasks);
                this.partitions = partitioner.getHistogramPartitions(histogram.getSortedRecord(), histogram.getTotalKmerCount());
            } else {
                LOG.info("Use equal range partitioning");
                KmerRangePartitioner partitioner = new KmerRangePartitioner(this.ppConfig.getKmerSize(), numReduceTasks);
                this.partitions = partitioner.getEqualRangePartitions();
            }
            
            this.partitionEndKeys = new CompressedSequenceWritable[this.partitions.length];
            for (int i = 0; i < this.partitions.length; i++) {
                try {
                    this.partitionEndKeys[i] = new CompressedSequenceWritable(this.partitions[i].getPartitionEndKmer());
                } catch (IOException ex) {
                    throw new RuntimeException(ex.toString());
                }
            }
        }
    }
    
    @Override
    public int getPartition(CompressedSequenceWritable key, IntArrayWritable value, int numReduceTasks) {
        if(!this.initialized) {
            try {
                initialize();
                this.initialized = true;
            } catch (IOException ex) {
                throw new RuntimeException(ex.toString());
            }
        }
        
        try {
            initializeSecond(numReduceTasks);
        } catch (IOException ex) {
            throw new RuntimeException(ex.toString());
        }
        
        int partition = getPartitionIndex(key);
        if(partition < 0) {
            throw new RuntimeException("partition failed");
        }
        
        return partition;
    }

    private int getPartitionIndex(CompressedSequenceWritable key) {
        int idx = Arrays.binarySearch(this.partitionEndKeys, 0, this.partitionEndKeys.length, key, new CompressedSequenceWritableComparator());
        if(idx >= 0) {
            //exact match
            return idx;      
        } else {
            return -(idx + 1);
        }
    }
}
