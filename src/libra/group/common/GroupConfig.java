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
package libra.group.common;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import libra.common.json.JsonSerializer;
import libra.preprocess.common.PreprocessorConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class GroupConfig {
    
    public static final long DEFAULT_GROUPSIZE = PreprocessorConfig.DEFAULT_GROUPSIZE;
    public static final int DEFAULT_MAX_GROUPNUM = PreprocessorConfig.DEFAULT_MAX_GROUPNUM;
    
    protected static final String HADOOP_CONFIG_KEY = "libra.group.common.groupconfig";
    
    private long groupSize = DEFAULT_GROUPSIZE;
    private int maxGroupNum = DEFAULT_MAX_GROUPNUM;
    private List<String> sequencePaths = new ArrayList<String>();
    
    public static GroupConfig createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (GroupConfig) serializer.fromJsonFile(file, GroupConfig.class);
    }
    
    public static GroupConfig createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (GroupConfig) serializer.fromJson(json, GroupConfig.class);
    }
    
    public static GroupConfig createInstance(Configuration conf) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (GroupConfig) serializer.fromJsonConfiguration(conf, HADOOP_CONFIG_KEY, GroupConfig.class);
    }
    
    public static GroupConfig createInstance(FileSystem fs, Path file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (GroupConfig) serializer.fromJsonFile(fs, file, GroupConfig.class);
    }
    
    public GroupConfig() {
        
    }
    
    public GroupConfig(GroupConfig config) {
        this.groupSize = config.groupSize;
        this.maxGroupNum = config.maxGroupNum;
        this.sequencePaths = new ArrayList<String>();
        this.sequencePaths.addAll(config.sequencePaths);
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
