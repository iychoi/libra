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
import libra.common.hadoop.io.datatypes.CompressedSequenceWritable;
import libra.common.json.JsonSerializer;
import libra.common.kmermatch.KmerMatchFileMapping;
import libra.common.kmermatch.KmerMatchResult;
import libra.core.commom.CoreConfig;
import libra.core.common.kmersimilarity.KmerSimilarityOutputRecord;
import libra.preprocess.common.WeightAlgorithm;
import libra.preprocess.common.helpers.KmerIndexHelper;
import libra.preprocess.common.helpers.KmerStatisticsHelper;
import libra.preprocess.common.kmerstatistics.KmerStatistics;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author iychoi
 */
@SuppressWarnings({"deprecation","cast"})
public class KmerSimilarityMapper extends Mapper<CompressedSequenceWritable, KmerMatchResult, Text, Text> {
    
    private static final Log LOG = LogFactory.getLog(KmerSimilarityMapper.class);
    
    private CoreConfig libraConfig;
    private KmerMatchFileMapping fileMapping;
    private Hashtable<String, Integer> idCacheTable;
    private Counter reportCounter;
    private JsonSerializer serializer;
    
    private int valuesLen;
    private double[] scoreAccumulated;
    private WeightAlgorithm weightAlgorithm;
    private double[] tfConsineNormBase;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.libraConfig = CoreConfig.createInstance(context.getConfiguration());
        this.fileMapping = KmerMatchFileMapping.createInstance(context.getConfiguration());
        
        this.idCacheTable = new Hashtable<String, Integer>();
        
        this.reportCounter = context.getCounter("KmerSimilarity", "report");
        this.serializer = new JsonSerializer();
        
        this.valuesLen = this.fileMapping.getSize();
        this.scoreAccumulated = new double[this.valuesLen * this.valuesLen];
        for(int i=0;i<this.scoreAccumulated.length;i++) {
            this.scoreAccumulated[i] = 0;
        }
        
        this.weightAlgorithm = this.libraConfig.getWeightAlgorithm();
        if(this.weightAlgorithm == null) {
            this.weightAlgorithm = CoreConfig.DEFAULT_WEIGHT_ALGORITHM;
        }
        
        this.tfConsineNormBase = new double[this.valuesLen];
        for(int i=0;i<this.tfConsineNormBase.length;i++) {
            // fill tfConsineNormBase
            String sequenceFilename = this.fileMapping.getSequenceFileFromID(i);
            String statisticsFilename = KmerStatisticsHelper.makeKmerStatisticsFileName(sequenceFilename);
            Path statisticsPath = new Path(this.libraConfig.getKmerStatisticsPath(), statisticsFilename);
            FileSystem fs = statisticsPath.getFileSystem(context.getConfiguration());
            
            KmerStatistics statistics = KmerStatistics.createInstance(fs, statisticsPath);

            switch(this.weightAlgorithm) {
                case LOGALITHM:
                    this.tfConsineNormBase[i] = statistics.getLogTFCosineNormBase();
                    break;
                case NATURAL:
                    this.tfConsineNormBase[i] = statistics.getNaturalTFCosineNormBase();
                    break;
                case BOOLEAN:
                    this.tfConsineNormBase[i] = statistics.getBooleanTFCosineNormBase();
                    break;
                default:
                    LOG.info("Unknown algorithm specified : " + this.weightAlgorithm.toString());
                    throw new IOException("Unknown algorithm specified : " + this.weightAlgorithm.toString());
            }
        }
    }
    
    private double getTFWeight(int freq) throws IOException {
        switch(this.weightAlgorithm) {
            case LOGALITHM:
                return (double)(1 + Math.log10(freq));
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
        IntWritable[] valueArray = value.getVals();
        Path[] kmerIndexPathArray = value.getKmerIndexPath();
        
        // filter out empty values
        ArrayList<IntWritable> filteredValueArray = new ArrayList<IntWritable>();
        ArrayList<Path> filteredKmerIndexPathArray = new ArrayList<Path>();
        
        for(int i=0;i<valueArray.length;i++) {
            if(valueArray[i] != null) {
                filteredValueArray.add(valueArray[i]);
                filteredKmerIndexPathArray.add(kmerIndexPathArray[i]);
            }
        }
        
        valueArray = null;
        kmerIndexPathArray = null;
        
        if(filteredValueArray.size() <= 1) {
            // skip
            return;
        }
        
        int[] fileid_arr = new int[filteredValueArray.size()];
        
        for(int i=0;i<filteredValueArray.size();i++) {
            int fileidInt = 0;
            String indexFilename = filteredKmerIndexPathArray.get(i).getName();
            Integer fileid = this.idCacheTable.get(indexFilename);
            if(fileid == null) {
                String sequenceFilename = KmerIndexHelper.getSequenceFileName(indexFilename);
                int id = this.fileMapping.getIDFromSequenceFile(sequenceFilename);
                this.idCacheTable.put(indexFilename, id);
                fileidInt = id;
            } else {
                fileidInt = fileid.intValue();
            }
            
            fileid_arr[i] = fileidInt;
        }
        
        // compute normal
        double[] normal = new double[this.valuesLen];
        for(int i=0;i<this.valuesLen;i++) {
            normal[i] = 0;
        }
        
        for(int i=0;i<filteredValueArray.size();i++) {
            IntWritable arr = filteredValueArray.get(i);
            int freq = arr.get();
            double weight = getTFWeight(freq);
            normal[fileid_arr[i]] = weight / this.tfConsineNormBase[fileid_arr[i]];
        }
        
        accumulateScore(normal);
        
        this.reportCounter.increment(1);
    }
    
    private void accumulateScore(double[] normal) {
        for(int i=0;i<this.valuesLen;i++) {
            for(int j=0;j<this.valuesLen;j++) {
                this.scoreAccumulated[i*this.valuesLen + j] += normal[i] * normal[j];
            }
        }
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        KmerSimilarityOutputRecord rec = new KmerSimilarityOutputRecord();
        rec.setScore(this.scoreAccumulated);
                
        String json = this.serializer.toJson(rec);
        context.write(new Text(" "), new Text(json));

        this.fileMapping = null;
        this.idCacheTable.clear();
        this.idCacheTable = null;
    
        this.libraConfig = null;
        this.scoreAccumulated = null;
        this.tfConsineNormBase = null;
        
        this.serializer = null;
    }
}
