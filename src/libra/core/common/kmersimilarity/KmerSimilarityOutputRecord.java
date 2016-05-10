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
package libra.core.common.kmersimilarity;

import java.io.File;
import java.io.IOException;
import libra.common.json.JsonSerializer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class KmerSimilarityOutputRecord {
    
    private static final Log LOG = LogFactory.getLog(KmerSimilarityOutputRecord.class);
    
    private static final String HADOOP_CONFIG_KEY = "libra.core.common.kmersimilarity.kmersimilarityoutputrecord";
    
    private double[] similarityScore;
    
    public static KmerSimilarityOutputRecord createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerSimilarityOutputRecord) serializer.fromJsonFile(file, KmerSimilarityOutputRecord.class);
    }
    
    public static KmerSimilarityOutputRecord createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerSimilarityOutputRecord) serializer.fromJson(json, KmerSimilarityOutputRecord.class);
    }
    
    public static KmerSimilarityOutputRecord createInstance(Configuration conf) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerSimilarityOutputRecord) serializer.fromJsonConfiguration(conf, HADOOP_CONFIG_KEY, KmerSimilarityOutputRecord.class);
    }
    
    public static KmerSimilarityOutputRecord createInstance(FileSystem fs, Path file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerSimilarityOutputRecord) serializer.fromJsonFile(fs, file, KmerSimilarityOutputRecord.class);
    }
    
    public KmerSimilarityOutputRecord() {
    }
    
    @JsonProperty("score")
    public double[] getScore() {
        return this.similarityScore;
    }
    
    @JsonProperty("score")
    public void setScore(double[] score) {
        this.similarityScore = score;
    }
    
    @JsonIgnore
    public void addScore(double[] score) {
        if(this.similarityScore == null) {
            this.similarityScore = score;
        }
        
        for(int i=0;i<score.length;i++) {
            this.similarityScore[i] += score[i];
        }
    }
    
    @JsonIgnore
    public void saveTo(Configuration conf) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        serializer.toJsonConfiguration(conf, HADOOP_CONFIG_KEY, this);
    }
    
    @JsonIgnore
    public void saveTo(FileSystem fs, Path file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        serializer.toJsonFile(fs, file, this);
    }
}
