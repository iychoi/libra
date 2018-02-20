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
package libra.preprocess.common.samplegroup;

import java.io.File;
import java.io.IOException;
import libra.common.json.JsonSerializer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class SampleInfo {
    
    private static final Log LOG = LogFactory.getLog(SampleInfo.class);
    
    private static final String HADOOP_CONFIG_KEY = "libra.preprocess.common.samplegroup.sampleinfo";
    
    private String path;
    private long size;
    
    public static SampleInfo createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (SampleInfo) serializer.fromJsonFile(file, SampleInfo.class);
    }
    
    public static SampleInfo createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (SampleInfo) serializer.fromJson(json, SampleInfo.class);
    }
    
    public static SampleInfo createInstance(Configuration conf) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (SampleInfo) serializer.fromJsonConfiguration(conf, HADOOP_CONFIG_KEY, SampleInfo.class);
    }
    
    public static SampleInfo createInstance(FileSystem fs, Path file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (SampleInfo) serializer.fromJsonFile(fs, file, SampleInfo.class);
    }
    
    public SampleInfo() {
    }
    
    public SampleInfo(String path, long size) {
        this.path = path;
        this.size = size;
    }
    
    public SampleInfo(FileStatus status) {
        this.path = status.getPath().toString();
        this.size = status.getLen();
    }
    
    @JsonProperty("path")
    public String getPath() {
        return this.path;
    }
    
    @JsonProperty("path")
    public void setPath(String path) {
        this.path = path;
    }
    
    @JsonProperty("size")
    public long getSize() {
        return this.size;
    }
    
    @JsonProperty("size")
    public void setSize(long size) {
        this.size = size;
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
