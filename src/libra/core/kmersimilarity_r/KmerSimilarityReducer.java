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
import libra.common.hadoop.io.datatypes.CompressedIntArrayWritable;
import libra.common.hadoop.io.datatypes.CompressedSequenceWritable;
import libra.common.json.JsonSerializer;
import libra.common.kmermatch.KmerMatchFileMapping;
import libra.core.commom.CoreConfig;
import libra.core.common.kmersimilarity.KmerSimilarityOutputRecord;
import libra.preprocess.common.helpers.KmerStatisticsHelper;
import libra.preprocess.common.kmerstatistics.KmerStatistics;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author iychoi
 */
@SuppressWarnings("cast")
public class KmerSimilarityReducer extends Reducer<CompressedSequenceWritable, CompressedIntArrayWritable, Text, Text> {
    
    private static final Log LOG = LogFactory.getLog(KmerSimilarityReducer.class);
    
    private CoreConfig libraConfig;
    private JsonSerializer serializer;
    private KmerMatchFileMapping fileMapping;
    private int valuesLen;
    private double[] scoreAccumulated;
    private double[] tfConsineNormBase;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.libraConfig = CoreConfig.createInstance(context.getConfiguration());
        this.serializer = new JsonSerializer();
        
        this.fileMapping = KmerMatchFileMapping.createInstance(context.getConfiguration());
        
        this.valuesLen = this.fileMapping.getSize();
        this.scoreAccumulated = new double[this.valuesLen * this.valuesLen];
        for(int i=0;i<this.scoreAccumulated.length;i++) {
            this.scoreAccumulated[i] = 0;
        }
        
        this.tfConsineNormBase = new double[this.valuesLen];
        for(int i=0;i<this.tfConsineNormBase.length;i++) {
            // fill tfConsineNormBase
            String fastaFilename = this.fileMapping.getFastaFileFromID(i);
            String statisticsFilename = KmerStatisticsHelper.makeKmerStatisticsFileName(fastaFilename);
            Path statisticsPath = new Path(this.libraConfig.getKmerStatisticsPath(), statisticsFilename);
            FileSystem fs = statisticsPath.getFileSystem(context.getConfiguration());
            
            KmerStatistics statistics = KmerStatistics.createInstance(fs, statisticsPath);

            this.tfConsineNormBase[i] = statistics.getTFCosineNormBase();
        }
    }
    
    @Override
    protected void reduce(CompressedSequenceWritable key, Iterable<CompressedIntArrayWritable> values, Context context) throws IOException, InterruptedException {
        // compute normal
        double[] normal = new double[this.valuesLen];
        for(int i=0;i<this.valuesLen;i++) {
            normal[i] = 0;
        }
        
        for(CompressedIntArrayWritable value : values) {
            int[] arr = value.get();
            int file_id = arr[0];
            int freq = arr[1];
            double tf = 1 + Math.log10(freq);
            normal[file_id] = ((double)tf) / this.tfConsineNormBase[file_id];
        }
        
        accumulateScore(normal);
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

        this.libraConfig = null;
        this.serializer = null;
        
        this.fileMapping = null;
    }
}
