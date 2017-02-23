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
package libra.core.commom;

import java.io.File;
import java.io.IOException;
import libra.common.helpers.PathHelper;
import libra.common.json.JsonSerializer;
import libra.preprocess.common.PreprocessorConfig;
import libra.preprocess.common.WeightAlgorithm;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonTypeInfo;

/**
 *
 * @author iychoi
 */
@JsonTypeInfo(use=JsonTypeInfo.Id.CLASS, include=JsonTypeInfo.As.PROPERTY, property="@class")
public class CoreConfig {
    
    public static final String DEFAULT_OUTPUT_PATH = "./libra_output";
    public static WeightAlgorithm DEFAULT_WEIGHT_ALGORITHM = WeightAlgorithm.LOGALITHM;
    
    private static final String HADOOP_CONFIG_KEY = "libra.core.common.coreconfig";
    
    private String reportFilePath;
    
    private String kmerHistogramPath;
    private String kmerIndexPath;
    private String kmerStatisticsPath;
    private WeightAlgorithm weightAlgorithm = WeightAlgorithm.LOGALITHM;
    private String outputPath = DEFAULT_OUTPUT_PATH;
    
    public static CoreConfig createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (CoreConfig) serializer.fromJsonFile(file, CoreConfig.class);
    }
    
    public static CoreConfig createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (CoreConfig) serializer.fromJson(json, CoreConfig.class);
    }
    
    public static CoreConfig createInstance(Configuration conf) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (CoreConfig) serializer.fromJsonConfiguration(conf, HADOOP_CONFIG_KEY, CoreConfig.class);
    }
    
    public static CoreConfig createInstance(FileSystem fs, Path file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (CoreConfig) serializer.fromJsonFile(fs, file, CoreConfig.class);
    }
    
    public CoreConfig() {
        
    }

    @JsonIgnore
    public void setPreprocessOutputRootPath(String preprocessOutputRootPath) {
        this.kmerHistogramPath = PathHelper.concatPath(preprocessOutputRootPath, PreprocessorConfig.DEFAULT_KMER_HISTOGRAM_PATH);
        this.kmerIndexPath = PathHelper.concatPath(preprocessOutputRootPath, PreprocessorConfig.DEFAULT_KMER_INDEX_PATH);
        this.kmerStatisticsPath = PathHelper.concatPath(preprocessOutputRootPath, PreprocessorConfig.DEFAULT_KMER_STATISITCS_PATH);
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
    
    @JsonProperty("weight_algorithm")
    public WeightAlgorithm getWeightAlgorithm() {
        return this.weightAlgorithm;
    }
    
    @JsonProperty("weight_algorithm")
    public void setWeightAlgorithm(WeightAlgorithm weightAlgorithm) {
        this.weightAlgorithm = weightAlgorithm;
    }
    
    @JsonProperty("output_path")
    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }
    
    @JsonProperty("output_path")
    public String getOutputPath() {
        return this.outputPath;
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
