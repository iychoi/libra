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
package libra.core.kmersimilarity_m;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import libra.common.hadoop.io.datatypes.IntArrayWritable;
import libra.common.hadoop.io.datatypes.CompressedSequenceWritable;
import libra.common.kmermatch.KmerMatchFileMapping;
import libra.common.kmermatch.KmerMatchResult;
import libra.core.common.CoreConfig;
import libra.core.common.ScoreAlgorithm;
import libra.core.common.kmersimilarity.KmerSimilarityResultPartRecord;
import libra.core.common.WeightAlgorithm;
import libra.core.common.kmersimilarity.KmerSimilarityResultPartRecordGroup;
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
    
    private static final double log2 = Math.log(2);
    
    private CoreConfig cConfig;
    private FileTable[] fileTables;
    private String[][] samplesInFileTable;
    private KmerMatchFileMapping fileMapping;
    
    private double[] scoreAccumulated;
    private WeightAlgorithm weightAlgorithm;
    private ScoreAlgorithm scoreAlgorithm;
    private double[] base;
    
    Hashtable<String, Integer> fileTableIDConvTable = new Hashtable<String, Integer>();
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        
        this.cConfig = CoreConfig.createInstance(conf);
        this.fileTables = this.cConfig.getFileTable().toArray(new FileTable[0]);
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
        
        this.weightAlgorithm = this.cConfig.getWeightAlgorithm();
        if(this.weightAlgorithm == null) {
            this.weightAlgorithm = CoreConfig.DEFAULT_WEIGHT_ALGORITHM;
        }
        
        this.scoreAlgorithm = this.cConfig.getScoreAlgorithm();
        if(this.scoreAlgorithm == null) {
            this.scoreAlgorithm = CoreConfig.DEFAULT_SCORE_ALGORITHM;
        }
        
        this.base = new double[valuesLen];
        int idx = 0;
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
                
                switch(this.scoreAlgorithm) {
                    case COSINESIMILARITY:
                        {
                            switch(this.weightAlgorithm) {
                                case LOGALITHM:
                                    this.base[idx] = statistics.getLogTFCosineNormBase();
                                    break;
                                case NATURAL:
                                    this.base[idx] = statistics.getNaturalTFCosineNormBase();
                                    break;
                                case BOOLEAN:
                                    this.base[idx] = statistics.getBooleanTFCosineNormBase();
                                    break;
                                default:
                                    LOG.info("Unknown algorithm specified : " + this.weightAlgorithm.toString());
                                    throw new IOException("Unknown algorithm specified : " + this.weightAlgorithm.toString());
                            }
                        }
                        break;
                    case BRAYCURTIS:
                        {
                            switch(this.weightAlgorithm) {
                                case LOGALITHM:
                                    this.base[idx] = statistics.getLogTFSum();
                                    break;
                                case NATURAL:
                                    this.base[idx] = statistics.getNaturalTFSum();
                                    break;
                                case BOOLEAN:
                                    this.base[idx] = statistics.getBooleanTFSum();
                                    break;
                                default:
                                    LOG.info("Unknown algorithm specified : " + this.weightAlgorithm.toString());
                                    throw new IOException("Unknown algorithm specified : " + this.weightAlgorithm.toString());
                            }
                        }
                        break;
                    case JENSENSHANNON:
                        {
                            switch(this.weightAlgorithm) {
                                case LOGALITHM:
                                    this.base[idx] = statistics.getLogTFSum();
                                    break;
                                case NATURAL:
                                    this.base[idx] = statistics.getNaturalTFSum();
                                    break;
                                case BOOLEAN:
                                    this.base[idx] = statistics.getBooleanTFSum();
                                    break;
                                default:
                                    LOG.info("Unknown algorithm specified : " + this.weightAlgorithm.toString());
                                    throw new IOException("Unknown algorithm specified : " + this.weightAlgorithm.toString());
                            }
                        }
                        break;
                    default:
                        LOG.info("Unknown algorithm specified : " + this.scoreAlgorithm.toString());
                        throw new IOException("Unknown algorithm specified : " + this.scoreAlgorithm.toString());
                }

                idx++;
            }
        }
    }
    
    private double getTFWeight(int freq) throws IOException {
        switch(this.weightAlgorithm) {
            case LOGALITHM:
                return 1 + Math.log10(freq);
            case NATURAL:
                return freq;
            case BOOLEAN:
                if(freq > 0) {
                    return 1;
                }
                return 0;
            default:
                throw new IOException("Unknown algorithm specified : " + this.weightAlgorithm.toString());
        }
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
                double weight = getTFWeight(freq);
                score_array[file_id] = weight;
            }
        }

        accumulateScore(score_array);
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
    
    private void accumulateScore(double[] score_array) throws IOException {
        switch(this.scoreAlgorithm) {
            case COSINESIMILARITY:
                accumulateScoreCosineSimilarity(score_array);
                break;
            case BRAYCURTIS:
                accumulateScoreBrayCurtis(score_array);
                break;
            case JENSENSHANNON:
                accumulateScoreJensenShannon(score_array);
                break;
            default:
                LOG.info("Unknown algorithm specified : " + this.scoreAlgorithm.toString());
                throw new IOException("Unknown algorithm specified : " + this.scoreAlgorithm.toString());
        }
    }
    
    private void accumulateScoreCosineSimilarity(double[] score_array) throws IOException {
        int valuesLen = this.fileMapping.getSize();
        int nonZeroFields = 0;
        for(int i=0;i<valuesLen;i++) {
            if(score_array[i] != 0) {
                nonZeroFields++;
            }
        }
        
        int[] nonZeroScoresIdx = new int[nonZeroFields];
        double[] nonZeroScoresVal = new double[nonZeroFields];
        int idx = 0;
        for(int i=0;i<valuesLen;i++) {
            if(score_array[i] != 0) {
                nonZeroScoresIdx[idx] = i;
                nonZeroScoresVal[idx] = score_array[i] / this.base[i];
                idx++;
            }
        }
        
        for(int i=0;i<nonZeroFields;i++) {
            for(int j=0;j<nonZeroFields;j++) {
                this.scoreAccumulated[nonZeroScoresIdx[i]*valuesLen + nonZeroScoresIdx[j]] += nonZeroScoresVal[i] * nonZeroScoresVal[j];
            }
        }
    }
    
    private void accumulateScoreBrayCurtis(double[] score_array) throws IOException {
        int valuesLen = this.fileMapping.getSize();
        for(int i=0;i<valuesLen;i++) {
            for(int j=0;j<valuesLen;j++) {
                double base_two = this.base[i] + this.base[j];
                this.scoreAccumulated[i*valuesLen + j] += Math.abs(score_array[i] - score_array[j]) / base_two;
            }
        }
    }
    
    private void accumulateScoreJensenShannon(double[] score_array) throws IOException {
        int valuesLen = this.fileMapping.getSize();
        double[] score_array_new = new double[valuesLen];
        for(int i=0;i<valuesLen;i++) {
            score_array_new[i] = score_array[i] / this.base[i];
        }
        
        for(int i=0;i<valuesLen;i++) {
            for(int j=0;j<valuesLen;j++) {
                double avg = (score_array_new[i] + score_array_new[j]) / 2;
                if(avg != 0) {
                    this.scoreAccumulated[i*valuesLen + j] += ((score_array_new[i] * Math.log(score_array_new[i] / avg)) + (score_array_new[j] * Math.log(score_array_new[j] / avg))) / log2;
                }
            }
        }
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        int valuesLen = this.fileMapping.getSize();
        
        for(int i=0;i<valuesLen;i++) {
            KmerSimilarityResultPartRecordGroup group = new KmerSimilarityResultPartRecordGroup();
            for(int j=0;j<valuesLen;j++) {
                double score = 0;
                switch(this.scoreAlgorithm) {
                    case COSINESIMILARITY:
                        score = this.scoreAccumulated[i*valuesLen + j];
                        break;
                    case BRAYCURTIS:
                        score = this.scoreAccumulated[i*valuesLen + j];
                        break;
                    case JENSENSHANNON:
                        score = this.scoreAccumulated[i*valuesLen + j] / 2;
                        break;
                    default:
                        LOG.info("Unknown algorithm specified : " + this.scoreAlgorithm.toString());
                        throw new IOException("Unknown algorithm specified : " + this.scoreAlgorithm.toString());
                }
                
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
