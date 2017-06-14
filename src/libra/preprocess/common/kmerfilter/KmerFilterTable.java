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
package libra.preprocess.common.kmerfilter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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
public class KmerFilterTable {
    
    private static final Log LOG = LogFactory.getLog(KmerFilterTable.class);
    
    private static final String HADOOP_CONFIG_KEY = "libra.preprocess.common.kmerfilter.kmerfiltertable";
    
    private String name;
    private List<KmerFilter> filter = new ArrayList<KmerFilter>();
    
    public static KmerFilterTable createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerFilterTable) serializer.fromJsonFile(file, KmerFilterTable.class);
    }
    
    public static KmerFilterTable createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerFilterTable) serializer.fromJson(json, KmerFilterTable.class);
    }
    
    public static KmerFilterTable createInstance(Configuration conf) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerFilterTable) serializer.fromJsonConfiguration(conf, HADOOP_CONFIG_KEY, KmerFilterTable.class);
    }
    
    public static KmerFilterTable createInstance(FileSystem fs, Path file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerFilterTable) serializer.fromJsonFile(fs, file, KmerFilterTable.class);
    }
    
    public KmerFilterTable() {
    }
    
    public KmerFilterTable(String name) {
        this.name = name;
    }
    
    @JsonProperty("name")
    public String getName() {
        return this.name;
    }
    
    @JsonProperty("name")
    public void setName(String name) {
        this.name = name;
    }
    
    @JsonIgnore
    public int filterNum() {
        return this.filter.size();
    }
    
    @JsonProperty("filter")
    public Collection<KmerFilter> getFilter() {
        return this.filter;
    }
    
    @JsonProperty("filter")
    public void addFilter(Collection<KmerFilter> filter) {
        this.filter.addAll(filter);
    }
    
    @JsonIgnore
    public void addFilter(KmerFilter filter) {
        this.filter.add(filter);
    }
    
    @JsonIgnore
    public void clearFilter() {
        this.filter.clear();
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
