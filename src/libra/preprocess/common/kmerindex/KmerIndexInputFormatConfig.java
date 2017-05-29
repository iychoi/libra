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
package libra.preprocess.common.kmerindex;

import java.io.File;
import java.io.IOException;
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
public class KmerIndexInputFormatConfig {
    
    private static final String HADOOP_CONFIG_KEY = "libra.preprocess.common.kmerindex.kmerindexinputformatconfig";
    
    private int kmerSize;
    private String kmerIndexTableFilePath;
    
    public static KmerIndexInputFormatConfig createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerIndexInputFormatConfig) serializer.fromJsonFile(file, KmerIndexInputFormatConfig.class);
    }
    
    public static KmerIndexInputFormatConfig createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerIndexInputFormatConfig) serializer.fromJson(json, KmerIndexInputFormatConfig.class);
    }
    
    public static KmerIndexInputFormatConfig createInstance(Configuration conf) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerIndexInputFormatConfig) serializer.fromJsonConfiguration(conf, HADOOP_CONFIG_KEY, KmerIndexInputFormatConfig.class);
    }
    
    public static KmerIndexInputFormatConfig createInstance(FileSystem fs, Path file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerIndexInputFormatConfig) serializer.fromJsonFile(fs, file, KmerIndexInputFormatConfig.class);
    }
    
    public KmerIndexInputFormatConfig() {
        
    }
    
    @JsonProperty("kmer_size")
    public void setKmerSize(int kmerSize) {
        this.kmerSize = kmerSize;
    }
    
    @JsonProperty("kmer_size")
    public int getKmerSize() {
        return this.kmerSize;
    }
    
    @JsonProperty("kmer_index_table_file_path")
    public void setKmerIndexTableFilePath(String kmerIndexIndexPath) {
        this.kmerIndexTableFilePath = kmerIndexIndexPath;
    }
    
    @JsonProperty("kmer_index_table_file_path")
    public String getKmerIndexTableFilePath() {
        return this.kmerIndexTableFilePath;
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
