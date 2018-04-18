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
package libra.distancematrix.common;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import libra.common.helpers.PathHelper;
import libra.common.json.JsonSerializer;
import libra.preprocess.common.PreprocessorConfig;
import libra.preprocess.common.filetable.FileTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class DistanceMatrixConfig {
    
    public static final String DEFAULT_OUTPUT_PATH = "./libra_output";
    public static WeightAlgorithm DEFAULT_WEIGHT_ALGORITHM = WeightAlgorithm.LOGARITHM;
    public static ScoreAlgorithm DEFAULT_SCORE_ALGORITHM = ScoreAlgorithm.COSINESIMILARITY;
    public static RunMode DEFAULT_RUN_MODE = RunMode.MAP;
    public static final int DEFAULT_TASKNUM = PreprocessorConfig.DEFAULT_TASKNUM; // user system default
    public static final boolean DEFAULT_USE_HISTOGRAM = PreprocessorConfig.DEFAULT_USE_HISTOGRAM;
    
    private static final String HADOOP_CONFIG_KEY = "libra.distancematrix.common.distancematrixconfig";
    
    private String reportFilePath;
    
    private int taskNum = DEFAULT_TASKNUM;
    private boolean useHistogram = DEFAULT_USE_HISTOGRAM;
    private String fileTablePath;
    private String kmerHistogramPath;
    private String kmerIndexPath;
    private String kmerStatisticsPath;
    private WeightAlgorithm weightAlgorithm = WeightAlgorithm.LOGARITHM;
    private ScoreAlgorithm scoreAlgorithm = ScoreAlgorithm.COSINESIMILARITY;
    private RunMode runMode = RunMode.MAP;
    private String outputPath = DEFAULT_OUTPUT_PATH;
    
    private List<FileTable> fileTables = new ArrayList<FileTable>();
    
    public static DistanceMatrixConfig createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (DistanceMatrixConfig) serializer.fromJsonFile(file, DistanceMatrixConfig.class);
    }
    
    public static DistanceMatrixConfig createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (DistanceMatrixConfig) serializer.fromJson(json, DistanceMatrixConfig.class);
    }
    
    public static DistanceMatrixConfig createInstance(Configuration conf) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (DistanceMatrixConfig) serializer.fromJsonConfiguration(conf, HADOOP_CONFIG_KEY, DistanceMatrixConfig.class);
    }
    
    public static DistanceMatrixConfig createInstance(FileSystem fs, Path file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (DistanceMatrixConfig) serializer.fromJsonFile(fs, file, DistanceMatrixConfig.class);
    }
    
    public DistanceMatrixConfig() {
        
    }

    @JsonIgnore
    public void setPreprocessRootPath(String preprocessRootPath) {
        this.fileTablePath = PathHelper.concatPath(preprocessRootPath, PreprocessorConfig.DEFAULT_FILE_TABLE_PATH);
        this.kmerHistogramPath = PathHelper.concatPath(preprocessRootPath, PreprocessorConfig.DEFAULT_KMER_HISTOGRAM_PATH);
        this.kmerIndexPath = PathHelper.concatPath(preprocessRootPath, PreprocessorConfig.DEFAULT_KMER_INDEX_PATH);
        this.kmerStatisticsPath = PathHelper.concatPath(preprocessRootPath, PreprocessorConfig.DEFAULT_KMER_STATISITCS_PATH);
    }
    
    @JsonProperty("task_num")
    public int getTaskNum() {
        return this.taskNum;
    }
    
    @JsonProperty("task_num")
    public void setTaskNum(int taskNum) {
        this.taskNum = taskNum;
    }
    
    @JsonProperty("use_histogram")
    public boolean getUseHistogram() {
        return this.useHistogram;
    }
    
    @JsonProperty("use_histogram")
    public void setUseHistogram(boolean useHistogram) {
        this.useHistogram = useHistogram;
    }
    
    @JsonProperty("file_table_path")
    public String getFileTablePath() {
        return this.fileTablePath;
    }
    
    @JsonProperty("file_table_path")
    public void setFileTablePath(String fileTablePath) {
        this.fileTablePath = fileTablePath;
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
    
    @JsonProperty("score_algorithm")
    public ScoreAlgorithm getScoreAlgorithm() {
        return this.scoreAlgorithm;
    }
    
    @JsonProperty("score_algorithm")
    public void setScoreAlgorithm(ScoreAlgorithm scoreAlgorithm) {
        this.scoreAlgorithm = scoreAlgorithm;
    }
    
    @JsonProperty("run_mode")
    public RunMode getRunMode() {
        return this.runMode;
    }
    
    @JsonProperty("run_mode")
    public void setRunMode(RunMode runMode) {
        this.runMode = runMode;
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
    
    @JsonProperty("file_table")
    public Collection<FileTable> getFileTable() {
        return this.fileTables;
    }
    
    @JsonProperty("file_table")
    public void addFileTable(Collection<FileTable> fileTable) {
        this.fileTables.addAll(fileTable);
    }
    
    @JsonIgnore
    public void addFileTable(FileTable fileTable) {
        this.fileTables.add(fileTable);
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
