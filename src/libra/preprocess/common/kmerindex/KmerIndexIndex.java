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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
public class KmerIndexIndex {
    
    private static final Log LOG = LogFactory.getLog(KmerIndexIndex.class);
    
    private static final String HADOOP_CONFIG_KEY = "libra.preprocess.common.kmerindex.kmerindexindex";
    
    private List<String> lastKeyList = new ArrayList<String>();
    
    public static KmerIndexIndex createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerIndexIndex) serializer.fromJsonFile(file, KmerIndexIndex.class);
    }
    
    public static KmerIndexIndex createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerIndexIndex) serializer.fromJson(json, KmerIndexIndex.class);
    }
    
    public static KmerIndexIndex createInstance(Configuration conf) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerIndexIndex) serializer.fromJsonConfiguration(conf, HADOOP_CONFIG_KEY, KmerIndexIndex.class);
    }
    
    public static KmerIndexIndex createInstance(FileSystem fs, Path file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerIndexIndex) serializer.fromJsonFile(fs, file, KmerIndexIndex.class);
    }
    
    public KmerIndexIndex() {
    }
    
    @JsonIgnore
    public void addLastKey(String key) {
        this.lastKeyList.add(key);
    }
    
    @JsonProperty("last_keys")
    public void addLastKey(Collection<String> keys) {
        this.lastKeyList.addAll(keys);
    }
    
    @JsonIgnore
    public Collection<String> getLastKey() {
        return Collections.unmodifiableCollection(this.lastKeyList);
    }
    
    @JsonProperty("last_keys")
    public Collection<String> getSortedLastKeys() {
        Collections.sort(this.lastKeyList);
        return Collections.unmodifiableCollection(this.lastKeyList);
    }
    
    @JsonIgnore
    public int getSize() {
        return this.lastKeyList.size();
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
