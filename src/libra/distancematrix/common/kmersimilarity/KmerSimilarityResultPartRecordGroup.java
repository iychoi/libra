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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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
public class KmerSimilarityResultPartRecordGroup {
    
    private static final Log LOG = LogFactory.getLog(KmerSimilarityResultPartRecordGroup.class);
    
    private static final String HADOOP_CONFIG_KEY = "libra.core.common.kmersimilarity.kmersimilarityresultpartrecordgroup";
    
    private List<KmerSimilarityResultPartRecord> scores = new ArrayList<KmerSimilarityResultPartRecord>();
    
    public static KmerSimilarityResultPartRecordGroup createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerSimilarityResultPartRecordGroup) serializer.fromJsonFile(file, KmerSimilarityResultPartRecordGroup.class);
    }
    
    public static KmerSimilarityResultPartRecordGroup createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerSimilarityResultPartRecordGroup) serializer.fromJson(json, KmerSimilarityResultPartRecordGroup.class);
    }
    
    public static KmerSimilarityResultPartRecordGroup createInstance(Configuration conf) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerSimilarityResultPartRecordGroup) serializer.fromJsonConfiguration(conf, HADOOP_CONFIG_KEY, KmerSimilarityResultPartRecordGroup.class);
    }
    
    public static KmerSimilarityResultPartRecordGroup createInstance(FileSystem fs, Path file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerSimilarityResultPartRecordGroup) serializer.fromJsonFile(fs, file, KmerSimilarityResultPartRecordGroup.class);
    }
    
    public KmerSimilarityResultPartRecordGroup() {
    }
    
    @JsonProperty("score")
    public Collection<KmerSimilarityResultPartRecord> getScore() {
        return this.scores;
    }
    
    @JsonProperty("score")
    public void addScore(Collection<KmerSimilarityResultPartRecord> scores) {
        this.scores.addAll(scores);
    }
    
    @JsonIgnore
    public void addScore(KmerSimilarityResultPartRecord score) {
        this.scores.add(score);
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
