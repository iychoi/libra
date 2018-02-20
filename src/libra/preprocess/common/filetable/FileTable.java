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
package libra.preprocess.common.filetable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Hashtable;
import java.util.List;
import libra.common.json.JsonSerializer;
import libra.preprocess.common.samplegroup.SampleGroup;
import libra.preprocess.common.samplegroup.SampleInfo;
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
public class FileTable {
    
    private static final Log LOG = LogFactory.getLog(FileTable.class);
    
    private static final String HADOOP_CONFIG_KEY = "libra.preprocess.common.filetable.filetable";
    
    private String name;
    private int kmerSize;
    private List<String> samples = new ArrayList<String>();
    private Hashtable<String, Integer> sampleIDCacheTable = new Hashtable<String, Integer>();
    
    public static FileTable createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (FileTable) serializer.fromJsonFile(file, FileTable.class);
    }
    
    public static FileTable createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (FileTable) serializer.fromJson(json, FileTable.class);
    }
    
    public static FileTable createInstance(Configuration conf) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (FileTable) serializer.fromJsonConfiguration(conf, HADOOP_CONFIG_KEY, FileTable.class);
    }
    
    public static FileTable createInstance(FileSystem fs, Path file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (FileTable) serializer.fromJsonFile(fs, file, FileTable.class);
    }
    
    public FileTable() {
    }
    
    public FileTable(String name, int kmerSize) {
        this.name = name;
        this.kmerSize = kmerSize;
    }
    
    public FileTable(SampleGroup group, int kmerSize) {
        this.name = group.getName();
        this.kmerSize = kmerSize;
        
        Collection<SampleInfo> samples = group.getSamples();
        for(SampleInfo sample : samples) {
            this.samples.add(sample.getPath());
        }
    }
    
    @JsonProperty("name")
    public String getName() {
        return this.name;
    }
    
    @JsonProperty("name")
    public void setName(String name) {
        this.name = name;
    }
    
    @JsonProperty("kmer_size")
    public int getKmerSize() {
        return this.kmerSize;
    }
    
    @JsonProperty("kmer_size")
    public void setKmerSize(int kmerSize) {
        this.kmerSize = kmerSize;
    }
    
    @JsonIgnore
    public int samples() {
        return this.samples.size();
    }
    
    @JsonProperty("samples")
    public Collection<String> getSamples() {
        return this.samples;
    }
    
    @JsonIgnore
    public int getSampleID(String sample) {
        Integer sampleID = this.sampleIDCacheTable.get(sample);
        if(sampleID == null) {
            int sid = -1; // not found
            for(int i=0;i<this.samples.size();i++) {
                if(this.samples.get(i).compareTo(sample) == 0) {
                    sid = i;
                    break;
                }
            }
            
            if(sid < 0) {
                String sample_target_name = sample;
                int idx = sample_target_name.lastIndexOf("/");
                if(idx >= 0) {
                    sample_target_name = sample_target_name.substring(idx + 1);
                }
                
                for(int j=0;j<this.samples.size();j++) {
                    String sample_name = this.samples.get(j);
                    idx = sample_name.lastIndexOf("/");
                    if(idx >= 0) {
                        sample_name = sample_name.substring(idx + 1);
                    }
                    
                    if(sample_name.compareTo(sample_target_name) == 0) {
                        sid = j;
                        break;
                    }
                }
            }
            
            sampleID = sid;
            // cache
            this.sampleIDCacheTable.put(sample, sampleID);
        }
        return sampleID;
    }
    
    @JsonProperty("samples")
    public void addSample(Collection<String> samples) {
        this.samples.addAll(samples);
    }
    
    @JsonIgnore
    public void addSample(String sample) {
        this.samples.add(sample);
    }
    
    @JsonIgnore
    public void clearSample() {
        this.samples.clear();
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
