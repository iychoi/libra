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
package libra.preprocess.common;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import libra.common.helpers.PathHelper;
import libra.common.json.JsonSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class PreprocessorConfig {
    
    public static final int DEFAULT_KMERSIZE = 20;
    public static final String DEFAULT_OUTPUT_ROOT_PATH = "./libra_preprocess_output";
    public static final String DEFAULT_KMER_HISTOGRAM_PATH = "histogram";
    public static final String DEFAULT_KMER_INDEX_PATH = "kmerindex";
    public static final String DEFAULT_KMER_STATISITCS_PATH = "statistics";
    
    private static final String HADOOP_CONFIG_KEY = "libra.preprocess.common.preprocessorconfig";
    
    private String reportFilePath;
    
    private int kmerSize = DEFAULT_KMERSIZE;
    private ArrayList<String> fastaPaths = new ArrayList<String>();
    private String kmerHistogramPath = DEFAULT_OUTPUT_ROOT_PATH + "/" + DEFAULT_KMER_HISTOGRAM_PATH;
    private String kmerIndexPath = DEFAULT_OUTPUT_ROOT_PATH + "/" + DEFAULT_KMER_INDEX_PATH;
    private String kmerStatisticsPath = DEFAULT_OUTPUT_ROOT_PATH + "/" + DEFAULT_KMER_STATISITCS_PATH;
    
    public static PreprocessorConfig createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (PreprocessorConfig) serializer.fromJsonFile(file, PreprocessorConfig.class);
    }
    
    public static PreprocessorConfig createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (PreprocessorConfig) serializer.fromJson(json, PreprocessorConfig.class);
    }
    
    public static PreprocessorConfig createInstance(Configuration conf) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (PreprocessorConfig) serializer.fromJsonConfiguration(conf, HADOOP_CONFIG_KEY, PreprocessorConfig.class);
    }
    
    public static PreprocessorConfig createInstance(FileSystem fs, Path file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (PreprocessorConfig) serializer.fromJsonFile(fs, file, PreprocessorConfig.class);
    }
    
    public PreprocessorConfig() {
        
    }

    @JsonProperty("kmer_size")
    public int getKmerSize() {
        return this.kmerSize;
    }
    
    @JsonProperty("kmer_size")
    public void setKmerSize(int kmerSize) {
        this.kmerSize = kmerSize;
    }

    @JsonProperty("fasta_path")
    public Collection<String> getFastaPath() {
        return this.fastaPaths;
    }
    
    @JsonProperty("fasta_path")
    public void addFastaPath(Collection<String> fastaPaths) {
        this.fastaPaths.addAll(fastaPaths);
    }
    
    @JsonIgnore
    public void addFastaPath(String fastaPath) {
        this.fastaPaths.add(fastaPath);
    }

    @JsonIgnore
    public void setOutputRootPath(String outputRootPath) {
        this.kmerHistogramPath = PathHelper.concatPath(outputRootPath, DEFAULT_KMER_HISTOGRAM_PATH);
        this.kmerIndexPath = PathHelper.concatPath(outputRootPath, DEFAULT_KMER_INDEX_PATH);
        this.kmerStatisticsPath = PathHelper.concatPath(outputRootPath, DEFAULT_KMER_STATISITCS_PATH);
    }
    
    @JsonProperty("histogram_path")
    public String getKmerHistogramPath() {
        return this.kmerHistogramPath;
    }
    
    @JsonProperty("histogram_path")
    public void setKmerHistogramPath(String histogramPath) {
        this.kmerHistogramPath = histogramPath;
    }
    
    @JsonProperty("kmer_index_path")
    public String getKmerIndexPath() {
        return this.kmerIndexPath;
    }
    
    @JsonProperty("kmer_index_path")
    public void setKmerIndexPath(String kmerIndexPath) {
        this.kmerIndexPath = kmerIndexPath;
    }
    
    @JsonProperty("statistics_path")
    public String getKmerStatisticsPath() {
        return this.kmerStatisticsPath;
    }
    
    @JsonProperty("statistics_path")
    public void setKmerStatisticsPath(String kmerStatisticsPath) {
        this.kmerStatisticsPath = kmerStatisticsPath;
    }

    @JsonProperty("report_path")
    public void setReportPath(String reportFilePath) {
        this.reportFilePath = reportFilePath;
    }
    
    @JsonProperty("report_path")
    public String getReportPath() {
        return this.reportFilePath;
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
