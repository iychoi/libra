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
import libra.common.fasta.FastaRead;
import libra.preprocess.common.PreprocessorConfig;
import libra.preprocess.common.kmerhistogram.KmerHistogram;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 *
 * @author iychoi
 */
public class KmerHistogramBuilderMapper extends Mapper<LongWritable, FastaRead, NullWritable, NullWritable> {
    
    private static final Log LOG = LogFactory.getLog(KmerHistogramBuilderMapper.class);
    
    private PreprocessorConfig ppConfig;
    
    private KmerHistogram histogram;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        
        this.ppConfig = PreprocessorConfig.createInstance(conf);
        
        FileSplit inputSplit = (FileSplit)context.getInputSplit();
        
        this.histogram = new KmerHistogram(inputSplit.getPath().getName(), this.ppConfig.getKmerSize());
    }
    
    @Override
    protected void map(LongWritable key, FastaRead value, Context context) throws IOException, InterruptedException {
        this.histogram.takeSample(value.getSequence());
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        String sampleName = this.histogram.getSampleName();
        String histogramFileName = KmerHistogramHelper.makeKmerHistogramFileName(sampleName);

        LOG.info("create a k-mer histogram file : " + histogramFileName);
        Path histogramOutputFile = new Path(this.ppConfig.getKmerHistogramPath(), histogramFileName);
        FileSystem outputFileSystem = histogramOutputFile.getFileSystem(context.getConfiguration());

        this.histogram.saveTo(outputFileSystem, histogramOutputFile);
    }
}
