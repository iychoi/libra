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
    
    private String sampleName;
    private int kmerSize;
    private double tfCosnormBase;
    
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
    
    public KmerStatistics(String sampleName, int kmerSize) {
        this.sampleName = sampleName;
        this.kmerSize = kmerSize;
    }
    
    @JsonProperty("sample_name")
    public String getSampleName() {
        return this.sampleName;
    }
    
    @JsonProperty("sample_name")
    public void setSampleName(String sampleName) {
        this.sampleName = sampleName;
    }
    
    @JsonProperty("kmer_size")
    public int getKmerSize() {
        return this.kmerSize;
    }
    
    @JsonProperty("kmer_size")
    public void setKmerSize(int kmerSize) {
        this.kmerSize = kmerSize;
    }
    
    @JsonProperty("tf_cosnorm_base")
    public void setTFCosineNormBase(double tfCosnormBase) {
        this.tfCosnormBase = tfCosnormBase;
    }
    
    @JsonProperty("tf_cosnorm_base")
    public double getTFCosineNormBase() {
        return this.tfCosnormBase;
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
