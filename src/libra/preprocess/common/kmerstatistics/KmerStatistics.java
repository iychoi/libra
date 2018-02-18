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
public class KmerStatistics {
    
    private static final Log LOG = LogFactory.getLog(KmerStatistics.class);
    
    private static final String HADOOP_CONFIG_KEY = "libra.preprocess.common.kmerstatistics.kmerstatistics";
    
    private String name;
    private double logTFCosnormBase;
    private double naturalTFCosnormBase;
    private double booleanTFCosnormBase;
    private double logTFSum;
    private double naturalTFSum;
    private double booleanTFSum;
    
    public static KmerStatistics createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerStatistics) serializer.fromJsonFile(file, KmerStatistics.class);
    }
    
    public static KmerStatistics createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerStatistics) serializer.fromJson(json, KmerStatistics.class);
    }
    
    public static KmerStatistics createInstance(Configuration conf) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerStatistics) serializer.fromJsonConfiguration(conf, HADOOP_CONFIG_KEY, KmerStatistics.class);
    }
    
    public static KmerStatistics createInstance(FileSystem fs, Path file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerStatistics) serializer.fromJsonFile(fs, file, KmerStatistics.class);
    }
    
    public KmerStatistics() {
    }
    
    public KmerStatistics(String name) {
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
    
    @JsonProperty("log_tf_cosnorm_base")
    public void setLogTFCosineNormBase(double tfCosnormBase) {
        this.logTFCosnormBase = tfCosnormBase;
    }
    
    @JsonProperty("log_tf_cosnorm_base")
    public double getLogTFCosineNormBase() {
        return this.logTFCosnormBase;
    }
    
    @JsonIgnore
    public void incrementLogTFCosineNormBase(double tfCosnormBase) {
        this.logTFCosnormBase += tfCosnormBase;
    }
    
    @JsonProperty("natural_tf_cosnorm_base")
    public void setNaturalTFCosineNormBase(double tfCosnormBase) {
        this.naturalTFCosnormBase = tfCosnormBase;
    }
    
    @JsonProperty("natural_tf_cosnorm_base")
    public double getNaturalTFCosineNormBase() {
        return this.naturalTFCosnormBase;
    }
    
    @JsonIgnore
    public void incrementNaturalTFCosineNormBase(double tfCosnormBase) {
        this.naturalTFCosnormBase += tfCosnormBase;
    }
    
    @JsonProperty("boolean_tf_cosnorm_base")
    public void setBooleanTFCosineNormBase(double tfCosnormBase) {
        this.booleanTFCosnormBase = tfCosnormBase;
    }
    
    @JsonProperty("boolean_tf_cosnorm_base")
    public double getBooleanTFCosineNormBase() {
        return this.booleanTFCosnormBase;
    }
    
    @JsonIgnore
    public void incrementBooleanTFCosineNormBase(double tfCosnormBase) {
        this.booleanTFCosnormBase += tfCosnormBase;
    }
    
    @JsonProperty("boolean_tf_sum")
    public void setBooleanTFSum(double weight) {
        this.booleanTFSum = weight;
    }
    
    @JsonProperty("boolean_tf_sum")
    public double getBooleanTFSum() {
        return this.booleanTFSum;
    }
    
    @JsonIgnore
    public void incrementBooleanTFSum(double weight) {
        this.booleanTFSum += weight;
    }
    
    @JsonProperty("natural_tf_sum")
    public void setNaturalTFSum(double weight) {
        this.naturalTFSum = weight;
    }
    
    @JsonProperty("natural_tf_sum")
    public double getNaturalTFSum() {
        return this.naturalTFSum;
    }
    
    @JsonIgnore
    public void incrementNaturalTFSum(double weight) {
        this.naturalTFSum += weight;
    }
    
    @JsonProperty("log_tf_sum")
    public void setLogTFSum(double weight) {
        this.logTFSum = weight;
    }
    
    @JsonProperty("log_tf_sum")
    public double getLogTFSum() {
        return this.logTFSum;
    }
    
    @JsonIgnore
    public void incrementLogTFSum(double weight) {
        this.logTFSum += weight;
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
