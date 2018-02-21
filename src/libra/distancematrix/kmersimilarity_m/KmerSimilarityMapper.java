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
package libra.distancematrix.kmersimilarity_m;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import libra.common.hadoop.io.datatypes.IntArrayWritable;
import libra.common.hadoop.io.datatypes.CompressedSequenceWritable;
import libra.common.kmermatch.KmerMatchFileMapping;
import libra.common.kmermatch.KmerMatchResult;
import libra.distancematrix.common.DistanceMatrixConfig;
import libra.distancematrix.common.ScoreAlgorithm;
import libra.distancematrix.common.Weight;
import libra.distancematrix.common.kmersimilarity.KmerSimilarityResultPartRecord;
import libra.distancematrix.common.WeightAlgorithm;
import libra.distancematrix.common.kmersimilarity.AbstractScore;
import libra.distancematrix.common.kmersimilarity.KmerSimilarityResultPartRecordGroup;
import libra.distancematrix.common.kmersimilarity.ScoreFactory;
import libra.preprocess.common.filetable.FileTable;
import libra.preprocess.common.helpers.KmerIndexHelper;
import libra.preprocess.common.helpers.KmerStatisticsHelper;
import libra.preprocess.common.kmerstatistics.KmerStatistics;
import libra.preprocess.common.kmerstatistics.KmerStatisticsTable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author iychoi
 */
public class KmerSimilarityMapper extends Mapper<CompressedSequenceWritable, KmerMatchResult, Text, Text> {
    
    private static final Log LOG = LogFactory.getLog(KmerSimilarityMapper.class);
    
    private DistanceMatrixConfig dmConfig;
    private FileTable[] fileTables;
    private String[][] samplesInFileTable;
    private KmerMatchFileMapping fileMapping;
    
    private double[] scoreAccumulated;
    private WeightAlgorithm weightAlgorithm;
    private ScoreAlgorithm scoreAlgorithm;
    private AbstractScore scoreFunction;
    
    Hashtable<String, Integer> fileTableIDConvTable = new Hashtable<String, Integer>();
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        
        this.dmConfig = DistanceMatrixConfig.createInstance(conf);
        this.fileTables = this.dmConfig.getFileTable().toArray(new FileTable[0]);
        this.samplesInFileTable = new String[this.fileTables.length][];
        for(int i=0;i<this.fileTables.length;i++) {
            this.samplesInFileTable[i] = this.fileTables[i].getSamples().toArray(new String[0]);
        }
        
        this.fileMapping = KmerMatchFileMapping.createInstance(conf);
        
        int valuesLen = this.fileMapping.getSize();
        
        this.scoreAccumulated = new double[valuesLen * valuesLen];
        for(int i=0;i<this.scoreAccumulated.length;i++) {
            this.scoreAccumulated[i] = 0;
        }
        
        this.weightAlgorithm = this.dmConfig.getWeightAlgorithm();
        if(this.weightAlgorithm == null) {
            this.weightAlgorithm = DistanceMatrixConfig.DEFAULT_WEIGHT_ALGORITHM;
        }
        
        this.scoreAlgorithm = this.dmConfig.getScoreAlgorithm();
        if(this.scoreAlgorithm == null) {
            this.scoreAlgorithm = DistanceMatrixConfig.DEFAULT_SCORE_ALGORITHM;
        }
        
        this.scoreFunction = ScoreFactory.getScore(this.scoreAlgorithm);
        
        int idx = 0;
        KmerStatistics[] statisticsArray = new KmerStatistics[valuesLen];
        for(FileTable fileTable : this.dmConfig.getFileTable()) {
            String statisticsTableFilename = KmerStatisticsHelper.makeKmerStatisticsTableFileName(fileTable.getName());
            Path statisticsTablePath = new Path(this.dmConfig.getKmerStatisticsPath(), statisticsTableFilename);
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
    protected void map(CompressedSequenceWritable key, KmerMatchResult value, Context context) throws IOException, InterruptedException {
        int valuesLen = this.fileMapping.getSize();
        
        IntArrayWritable[] valueArray = value.getVals();
        Path[] kmerIndexTablePathArray = value.getKmerIndexTablePaths();
        
        // filter out empty values
        List<IntArrayWritable> filteredValueArray = new ArrayList<IntArrayWritable>();
        List<Path> filteredKmerIndexPathArray = new ArrayList<Path>();
        
        for(int i=0;i<valueArray.length;i++) {
            if(valueArray[i] != null) {
                filteredValueArray.add(valueArray[i]);
                filteredKmerIndexPathArray.add(kmerIndexTablePathArray[i]);
            }
        }
        
        valueArray = null;
        kmerIndexTablePathArray = null;
        
        // compute normal
        double[] score_array = new double[valuesLen];
        for(int i=0;i<valuesLen;i++) {
            score_array[i] = 0;
        }
        
        for(int i=0;i<filteredValueArray.size();i++) {
            Path path = filteredKmerIndexPathArray.get(i);
            IntArrayWritable valueArr = filteredValueArray.get(i);
            
            String fileTableName = KmerIndexHelper.getFileTableName(path);
            
            int fileTableID = convertFileTableNameToFileTableID(fileTableName);
            
            int[] arr = valueArr.get();
            for(int j=0;j<arr.length/2;j++) {
                int file_id_in_table = arr[j*2];
                int freq = arr[j*2 + 1];
            
                int file_id = convertFileIDToGlobalFileID(fileTableID, file_id_in_table);
                double weight = Weight.getTFWeight(this.weightAlgorithm, freq);
                score_array[file_id] = weight;
            }
        }
        
        this.scoreFunction.contributeScore(valuesLen, this.scoreAccumulated, score_array);
    }
    
    private int convertFileTableNameToFileTableID(String fileTableName) {
        Integer fileTableID = this.fileTableIDConvTable.get(fileTableName);
        if(fileTableID == null) {
            for(int i=0;i<this.fileTables.length;i++) {
                String name = this.fileTables[i].getName();
                if(name.equals(fileTableName)) {
                    fileTableID = i;
                    this.fileTableIDConvTable.put(name, i);
                    break;
                }
            }
        }
        return fileTableID;
    }
    
    private int convertFileIDToGlobalFileID(int file_table_id, int file_id_in_table) throws IOException {
        String sampleName = this.samplesInFileTable[file_table_id][file_id_in_table];
        return this.fileMapping.getIDFromSequenceFile(sampleName);
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
