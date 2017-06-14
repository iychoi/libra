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
package libra.preprocess.common.kmerfilter;

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
public class KmerFilter {
    
    private static final Log LOG = LogFactory.getLog(KmerFilter.class);
    
    private static final String HADOOP_CONFIG_KEY = "libra.preprocess.common.kmerfilter.kmerfilter";
    
    private String name;
    private long totalKmers;
    private long uniqueKmers;
    private long sumOfSquare;
    
    public static KmerFilter createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerFilter) serializer.fromJsonFile(file, KmerFilter.class);
    }
    
    public static KmerFilter createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerFilter) serializer.fromJson(json, KmerFilter.class);
    }
    
    public static KmerFilter createInstance(Configuration conf) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerFilter) serializer.fromJsonConfiguration(conf, HADOOP_CONFIG_KEY, KmerFilter.class);
    }
    
    public static KmerFilter createInstance(FileSystem fs, Path file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerFilter) serializer.fromJsonFile(fs, file, KmerFilter.class);
    }
    
    public KmerFilter() {
    }
    
    public KmerFilter(String name) {
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
    
    @JsonProperty("total_kmers")
    public void setTotalKmers(long totalKmers) {
        this.totalKmers = totalKmers;
    }
    
    @JsonProperty("total_kmers")
    public long getTotalKmers() {
        return this.totalKmers;
    }
    
    @JsonIgnore
    public void incrementTotalKmers(long totalKmers) {
        this.totalKmers += totalKmers;
    }
    
    @JsonProperty("unique_kmers")
    public void setUniqueKmers(long uniqueKmers) {
        this.uniqueKmers = uniqueKmers;
    }
    
    @JsonProperty("unique_kmers")
    public long getUniqueKmers() {
        return this.uniqueKmers;
    }
    
    @JsonIgnore
    public void incrementUniqueKmers(long uniqueKmers) {
        this.uniqueKmers += uniqueKmers;
    }
    
    @JsonProperty("sum_of_square")
    public void setSumOfSquare(long sumOfSquare) {
        this.sumOfSquare = sumOfSquare;
    }
    
    @JsonProperty("sum_of_square")
    public long getSumOfSquare() {
        return this.sumOfSquare;
    }
    
    @JsonIgnore
    public void incrementSumOfSquare(double sumOfSquare) {
        this.sumOfSquare += sumOfSquare;
    }
    
    @JsonIgnore
    public double getMean() {
        return (double)this.totalKmers / (double)this.uniqueKmers;
    }
    
    @JsonIgnore
    public double getStddev() {
        double mean = (double)this.totalKmers / (double)this.uniqueKmers;
        double mean_square = Math.pow(mean, 2);
        double term = (double)this.sumOfSquare / (double)this.uniqueKmers;
        return Math.sqrt(term - mean_square);
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
