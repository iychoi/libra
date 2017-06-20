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
import java.util.List;
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
    public static final long DEFAULT_GROUPSIZE = (long)(1024 * 1024 * 1024) * 10; // 10GB
    public static final int DEFAULT_MAX_GROUPNUM = 20;
    public static final int DEFAULT_TASKNUM = 0; // user system default
    public static final String DEFAULT_OUTPUT_ROOT_PATH = "./libra_preprocess_output";
    public static final String DEFAULT_FILE_TABLE_PATH = "filetable";
    public static final String DEFAULT_KMER_FILTER_PATH = "filter";
    public static final String DEFAULT_KMER_HISTOGRAM_PATH = "histogram";
    public static final String DEFAULT_KMER_INDEX_PATH = "kmerindex";
    public static final String DEFAULT_KMER_STATISITCS_PATH = "statistics";
    public static FilterAlgorithm DEFAULT_FILTER_ALGORITHM = FilterAlgorithm.NOTUNIQUE;
    
    protected static final String HADOOP_CONFIG_KEY = "libra.preprocess.common.preprocessorconfig";
    
    private String reportFilePath;
    
    private int kmerSize = DEFAULT_KMERSIZE;
    private long groupSize = DEFAULT_GROUPSIZE;
    private int maxGroupNum = DEFAULT_MAX_GROUPNUM;
    private int taskNum = DEFAULT_TASKNUM;
    private FilterAlgorithm filterAlgorithm = FilterAlgorithm.NOTUNIQUE;
    private List<String> sequencePaths = new ArrayList<String>();
    private String fileTablePath = DEFAULT_OUTPUT_ROOT_PATH + "/" + DEFAULT_FILE_TABLE_PATH;
    private String kmerFilterPath = DEFAULT_OUTPUT_ROOT_PATH + "/" + DEFAULT_KMER_FILTER_PATH;
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
    
    public PreprocessorConfig(PreprocessorConfig config) {
        this.reportFilePath = config.reportFilePath;
        this.kmerSize = config.kmerSize;
        this.groupSize = config.groupSize;
        this.maxGroupNum = config.maxGroupNum;
        this.taskNum = config.taskNum;
        this.filterAlgorithm = config.filterAlgorithm;
        this.sequencePaths = new ArrayList<String>();
        this.sequencePaths.addAll(config.sequencePaths);
        this.fileTablePath = config.fileTablePath;
        this.kmerFilterPath = config.kmerFilterPath;
        this.kmerHistogramPath = config.kmerHistogramPath;
        this.kmerIndexPath = config.kmerIndexPath;
        this.kmerStatisticsPath = config.kmerStatisticsPath;
    }

    @JsonProperty("kmer_size")
    public int getKmerSize() {
        return this.kmerSize;
    }
    
    @JsonProperty("kmer_size")
    public void setKmerSize(int kmerSize) {
        this.kmerSize = kmerSize;
    }
    
    @JsonProperty("group_size")
    public long getGroupSize() {
        return this.groupSize;
    }
    
    @JsonProperty("group_size")
    public void setGroupSize(long groupSize) {
        this.groupSize = groupSize;
    }
    
    @JsonProperty("max_group_num")
    public int getMaxGroupNum() {
        return this.maxGroupNum;
    }
    
    @JsonProperty("max_group_num")
    public void setMaxGroupNum(int maxGroupNum) {
        this.maxGroupNum = maxGroupNum;
    }
    
    @JsonProperty("task_num")
    public int getTaskNum() {
        return this.taskNum;
    }
    
    @JsonProperty("task_num")
    public void setTaskNum(int taskNum) {
        this.taskNum = taskNum;
    }
    
    @JsonProperty("filter_algorithm")
    public FilterAlgorithm getFilterAlgorithm() {
        return this.filterAlgorithm;
    }
    
    @JsonProperty("filter_algorithm")
    public void setFilterAlgorithm(FilterAlgorithm filterAlgorithm) {
        this.filterAlgorithm = filterAlgorithm;
    }

    @JsonProperty("sequence_path")
    public Collection<String> getSequencePath() {
        return this.sequencePaths;
    }
    
    @JsonProperty("sequence_path")
    public void addSequencePath(Collection<String> sequencePaths) {
        this.sequencePaths.addAll(sequencePaths);
    }
    
    @JsonIgnore
    public void addSequencePath(String sequencePath) {
        this.sequencePaths.add(sequencePath);
    }
    
    @JsonIgnore
    public void clearSequencePath() {
        this.sequencePaths.clear();
    }

    @JsonIgnore
    public void setOutputRootPath(String outputRootPath) {
        this.fileTablePath = PathHelper.concatPath(outputRootPath, DEFAULT_FILE_TABLE_PATH);
        this.kmerHistogramPath = PathHelper.concatPath(outputRootPath, DEFAULT_KMER_HISTOGRAM_PATH);
        this.kmerFilterPath = PathHelper.concatPath(outputRootPath, DEFAULT_KMER_FILTER_PATH);
        this.kmerIndexPath = PathHelper.concatPath(outputRootPath, DEFAULT_KMER_INDEX_PATH);
        this.kmerStatisticsPath = PathHelper.concatPath(outputRootPath, DEFAULT_KMER_STATISITCS_PATH);
    }
    
    @JsonProperty("file_table_path")
    public String getFileTablePath() {
        return this.fileTablePath;
    }
    
    @JsonProperty("file_table_path")
    public void setFileTablePath(String fileTablePath) {
        this.fileTablePath = fileTablePath;
    }
    
    @JsonProperty("kmer_filter_path")
    public String getKmerFilterPath() {
        return this.kmerFilterPath;
    }
    
    @JsonProperty("kmer_filter_path")
    public void setKmerFilterPath(String kmerFilterPath) {
        this.kmerFilterPath = kmerFilterPath;
    }
    
    @JsonProperty("kmer_histogram_path")
    public String getKmerHistogramPath() {
        return this.kmerHistogramPath;
    }
    
    @JsonProperty("kmer_histogram_path")
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
    
    @JsonProperty("kmer_statistics_path")
    public String getKmerStatisticsPath() {
        return this.kmerStatisticsPath;
    }
    
    @JsonProperty("kmer_statistics_path")
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
