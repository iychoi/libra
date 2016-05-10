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
import libra.common.hadoop.io.datatypes.CompressedSequenceWritable;
import libra.common.json.JsonSerializer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class KmerMatchResult {
    
    private static final Log LOG = LogFactory.getLog(KmerMatchResult.class);
    
    private static final String HADOOP_CONFIG_KEY = "libra.common.kmermatch.kmermatchresult";
    
    private CompressedSequenceWritable key;
    private IntWritable[] vals;
    private Path[] kmerIndexPath;
    
    public static KmerMatchResult createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerMatchResult) serializer.fromJsonFile(file, KmerMatchResult.class);
    }
    
    public static KmerMatchResult createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerMatchResult) serializer.fromJson(json, KmerMatchResult.class);
    }
    
    public static KmerMatchResult createInstance(Configuration conf) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerMatchResult) serializer.fromJsonConfiguration(conf, HADOOP_CONFIG_KEY, KmerMatchResult.class);
    }
    
    public static KmerMatchResult createInstance(FileSystem fs, Path file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerMatchResult) serializer.fromJsonFile(fs, file, KmerMatchResult.class);
    }
    
    public KmerMatchResult() {
        
    }
    
    public KmerMatchResult(CompressedSequenceWritable key, IntWritable[] vals, Path[] kmerIndexPath) {
        this.key = key;
        this.vals = vals;
        this.kmerIndexPath = kmerIndexPath;
    }
    
    @JsonProperty("kmer")
    public String getKmer() {
        return this.key.getSequence();
    }
    
    @JsonIgnore
    public CompressedSequenceWritable getKey() {
        return this.key;
    }
    
    @JsonIgnore
    public IntWritable[] getVals() {
        return this.vals;
    }
    
    @JsonProperty("kmer_index_path")
    public Path[] getKmerIndexPath() {
        return this.kmerIndexPath;
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
