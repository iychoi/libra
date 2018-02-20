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
package libra.core.kmersimilarity_r;

import java.io.IOException;
import libra.common.hadoop.io.datatypes.IntArrayWritable;
import libra.common.hadoop.io.datatypes.CompressedSequenceWritable;
import libra.common.kmermatch.KmerMatchFileMapping;
import libra.core.common.CoreConfig;
import libra.core.common.ScoreAlgorithm;
import libra.core.common.Weight;
import libra.core.common.kmersimilarity.KmerSimilarityResultPartRecord;
import libra.core.common.WeightAlgorithm;
import libra.core.common.kmersimilarity.AbstractScore;
import libra.core.common.kmersimilarity.KmerSimilarityResultPartRecordGroup;
import libra.core.common.kmersimilarity.ScoreFactory;
import libra.preprocess.common.filetable.FileTable;
import libra.preprocess.common.helpers.KmerStatisticsHelper;
import libra.preprocess.common.kmerstatistics.KmerStatistics;
import libra.preprocess.common.kmerstatistics.KmerStatisticsTable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author iychoi
 */
public class KmerSimilarityReducer extends Reducer<CompressedSequenceWritable, IntArrayWritable, Text, Text> {
    
    private static final Log LOG = LogFactory.getLog(KmerSimilarityReducer.class);
    
    private CoreConfig cConfig;
    private KmerMatchFileMapping fileMapping;
    
    private double[] scoreAccumulated;
    private WeightAlgorithm weightAlgorithm;
    private ScoreAlgorithm scoreAlgorithm;
    private AbstractScore scoreFunction;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        this.cConfig = CoreConfig.createInstance(conf);
        
        this.fileMapping = KmerMatchFileMapping.createInstance(conf);
        
        int valuesLen = this.fileMapping.getSize();
        this.scoreAccumulated = new double[valuesLen * valuesLen];
        for(int i=0;i<this.scoreAccumulated.length;i++) {
            this.scoreAccumulated[i] = 0;
        }
        
        this.weightAlgorithm = this.cConfig.getWeightAlgorithm();
        if(this.weightAlgorithm == null) {
            this.weightAlgorithm = CoreConfig.DEFAULT_WEIGHT_ALGORITHM;
        }
        
        this.scoreAlgorithm = this.cConfig.getScoreAlgorithm();
        if(this.scoreAlgorithm == null) {
            this.scoreAlgorithm = CoreConfig.DEFAULT_SCORE_ALGORITHM;
        }
        
        this.scoreFunction = ScoreFactory.getScore(this.scoreAlgorithm);
        
        int idx = 0;
        KmerStatistics[] statisticsArray = new KmerStatistics[valuesLen];
        for(FileTable fileTable : this.cConfig.getFileTable()) {
            String statisticsTableFilename = KmerStatisticsHelper.makeKmerStatisticsTableFileName(fileTable.getName());
            Path statisticsTablePath = new Path(this.cConfig.getKmerStatisticsPath(), statisticsTableFilename);
            FileSystem fs = statisticsTablePath.getFileSystem(conf);

            KmerStatisticsTable statisticsTable = KmerStatisticsTable.createInstance(fs, statisticsTablePath);
            for(KmerStatistics statistics : statisticsTable.getStatistics()) {
                String sequenceFile = this.fileMapping.getSequenceFileFromID(idx);
                if(!sequenceFile.equals(statistics.getName())) {
                    throw new IOException(String.format("File order is not correct - %s ==> %s", sequenceFile, statistics.getName()));
                }
                
                statisticsArray[idx] = statistics;
                idx++;
            }
        }
        
        this.scoreFunction.setParam(valuesLen, this.weightAlgorithm, statisticsArray);
    }
    
    @Override
    protected void reduce(CompressedSequenceWritable key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {
        // compute normal
        int valuesLen = this.fileMapping.getSize();
        double[] score_array = new double[valuesLen];
        for(int i=0;i<valuesLen;i++) {
            score_array[i] = 0;
        }
        
        for(IntArrayWritable value : values) {
            int[] arr = value.get();
            for(int i=0;i<arr.length/2;i++) {
                int file_id = arr[i*2];
                int freq = arr[i*2 + 1];
                double weight = Weight.getTFWeight(this.weightAlgorithm, freq);
                score_array[file_id] = weight;
            }
        }
        
        this.scoreFunction.contributeScore(valuesLen, this.scoreAccumulated, score_array);
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        int valuesLen = this.fileMapping.getSize();
        
        for(int i=0;i<valuesLen;i++) {
            KmerSimilarityResultPartRecordGroup group = new KmerSimilarityResultPartRecordGroup();
            for(int j=0;j<valuesLen;j++) {
                double score = this.scoreAccumulated[i*valuesLen + j];
                if(score != 0) {
                    KmerSimilarityResultPartRecord rec = new KmerSimilarityResultPartRecord();
                    rec.setFile1ID(i);
                    rec.setFile2ID(j);
                    rec.setScore(score);
                    
                    group.addScore(rec);
                }
            }
            String json = group.toString();
            context.write(new Text(" "), new Text(json));
        }
    }
}
