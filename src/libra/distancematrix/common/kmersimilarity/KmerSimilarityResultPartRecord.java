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
package libra.distancematrix.common.kmersimilarity;

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
public class KmerSimilarityResultPartRecord {
    
    private static final Log LOG = LogFactory.getLog(KmerSimilarityResultPartRecord.class);
    
    private static final String HADOOP_CONFIG_KEY = "libra.core.common.kmersimilarity.kmersimilarityresultpartrecord";
    
    private int file1ID;
    private int file2ID;
    private double score;
    
    public static KmerSimilarityResultPartRecord createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerSimilarityResultPartRecord) serializer.fromJsonFile(file, KmerSimilarityResultPartRecord.class);
    }
    
    public static KmerSimilarityResultPartRecord createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerSimilarityResultPartRecord) serializer.fromJson(json, KmerSimilarityResultPartRecord.class);
    }
    
    public static KmerSimilarityResultPartRecord createInstance(Configuration conf) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerSimilarityResultPartRecord) serializer.fromJsonConfiguration(conf, HADOOP_CONFIG_KEY, KmerSimilarityResultPartRecord.class);
    }
    
    public static KmerSimilarityResultPartRecord createInstance(FileSystem fs, Path file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerSimilarityResultPartRecord) serializer.fromJsonFile(fs, file, KmerSimilarityResultPartRecord.class);
    }
    
    public KmerSimilarityResultPartRecord() {
    }
    
    @JsonProperty("file1_id")
    public int getFile1ID() {
        return this.file1ID;
    }
    
    @JsonProperty("file1_id")
    public void setFile1ID(int file1ID) {
        this.file1ID = file1ID;
    }
    
    @JsonProperty("file2_id")
    public int getFile2ID() {
        return this.file2ID;
    }
    
    @JsonProperty("file2_id")
    public void setFile2ID(int file2ID) {
        this.file2ID = file2ID;
    }
    
    @JsonProperty("score")
    public double getScore() {
        return this.score;
    }
    
    @JsonProperty("score")
    public void setScore(double score) {
        this.score = score;
    }
    
    @JsonIgnore
    public void addScore(double score) {
        this.score += score;
    }
    
    @JsonIgnore
    public String toJson() {
        try {
            JsonSerializer serializer = new JsonSerializer();
            return serializer.toJson(this);
        } catch(Exception ex) {
            return null;
        }
    }
    
    @JsonIgnore
    public String toString() {
        try {
            JsonSerializer serializer = new JsonSerializer();
            return serializer.toJson(this);
        } catch(Exception ex) {
            return null;
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
