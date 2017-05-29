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
package libra.preprocess.common.kmerstatistics;

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
public class KmerStatisticsPart {
    
    private static final Log LOG = LogFactory.getLog(KmerStatisticsPart.class);
    
    private static final String HADOOP_CONFIG_KEY = "libra.preprocess.common.kmerstatistics.kmerstatisticspart";
    
    private String name;
    private double logTFWeight;
    private double naturalTFWeight;
    private double booleanTFWeight;
    
    public static KmerStatisticsPart createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerStatisticsPart) serializer.fromJsonFile(file, KmerStatisticsPart.class);
    }
    
    public static KmerStatisticsPart createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerStatisticsPart) serializer.fromJson(json, KmerStatisticsPart.class);
    }
    
    public static KmerStatisticsPart createInstance(Configuration conf) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerStatisticsPart) serializer.fromJsonConfiguration(conf, HADOOP_CONFIG_KEY, KmerStatisticsPart.class);
    }
    
    public static KmerStatisticsPart createInstance(FileSystem fs, Path file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerStatisticsPart) serializer.fromJsonFile(fs, file, KmerStatisticsPart.class);
    }
    
    public KmerStatisticsPart() {
    }
    
    public KmerStatisticsPart(String name) {
        this.name = name;
    }
    
    @JsonProperty("name")
    public String getName() {
        return this.name;
    }
    
    @JsonProperty("name")
    public void setName(String name) {
        this.name = name;
    }
    
    @JsonProperty("log_tf_weight")
    public void setLogTFWeight(double weight) {
        this.logTFWeight = weight;
    }
    
    @JsonProperty("log_tf_weight")
    public double getLogTFWeight() {
        return this.logTFWeight;
    }
    
    @JsonIgnore
    public void incrementLogTFWeight(double weight) {
        this.logTFWeight += weight;
    }
    
    @JsonProperty("natural_tf_weight")
    public void setNaturalTFWeight(double weight) {
        this.naturalTFWeight = weight;
    }
    
    @JsonProperty("natural_tf_weight")
    public double getNaturalTFWeight() {
        return this.naturalTFWeight;
    }
    
    @JsonIgnore
    public void incrementNaturalTFWeight(double weight) {
        this.naturalTFWeight += weight;
    }
    
    @JsonProperty("boolean_tf_weight")
    public void setBooleanTFWeight(double weight) {
        this.booleanTFWeight = weight;
    }
    
    @JsonProperty("boolean_tf_weight")
    public double getBooleanTFWeight() {
        return this.booleanTFWeight;
    }
    
    @JsonIgnore
    public void incrementBooleanTFWeight(double weight) {
        this.booleanTFWeight += weight;
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
