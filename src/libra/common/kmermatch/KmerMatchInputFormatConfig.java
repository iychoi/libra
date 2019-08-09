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
package libra.common.kmermatch;

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
public class KmerMatchInputFormatConfig {
    
    private static final Log LOG = LogFactory.getLog(KmerMatchInputFormatConfig.class);
    
    private static final String HADOOP_CONFIG_KEY = "libra.common.kmermatch.kmermatchinputformatconfig";
    
    private int kmerSize;
    private String fileTablePath;
    private String kmerIndexPath;
    
    public static KmerMatchInputFormatConfig createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerMatchInputFormatConfig) serializer.fromJsonFile(file, KmerMatchInputFormatConfig.class);
    }
    
    public static KmerMatchInputFormatConfig createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerMatchInputFormatConfig) serializer.fromJson(json, KmerMatchInputFormatConfig.class);
    }
    
    public static KmerMatchInputFormatConfig createInstance(Configuration conf) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerMatchInputFormatConfig) serializer.fromJsonConfiguration(conf, HADOOP_CONFIG_KEY, KmerMatchInputFormatConfig.class);
    }
    
    public static KmerMatchInputFormatConfig createInstance(FileSystem fs, Path file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerMatchInputFormatConfig) serializer.fromJsonFile(fs, file, KmerMatchInputFormatConfig.class);
    }
    
    public KmerMatchInputFormatConfig() {
    }
    
    @JsonProperty("kmer_size")
    public void setKmerSize(int kmerSize) {
        this.kmerSize = kmerSize;
    }
    
    @JsonProperty("kmer_size")
    public int getKmerSize() {
        return this.kmerSize;
    }
    
    @JsonProperty("file_table_path")
    public String getFileTablePath() {
        return this.fileTablePath;
    }
    
    @JsonProperty("file_table_path")
    public void setFileTablePath(String fileTablePath) {
        this.fileTablePath = fileTablePath;
    }
    
    @JsonProperty("kmer_index_path")
    public String getKmerIndexPath() {
        return this.kmerIndexPath;
    }
    
    @JsonProperty("kmer_index_path")
    public void setKmerIndexPath(String kmerIndexPath) {
        this.kmerIndexPath = kmerIndexPath;
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
