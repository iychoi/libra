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
public class KmerIndexTable {
    
    private static final Log LOG = LogFactory.getLog(KmerIndexTable.class);
    
    private static final String HADOOP_CONFIG_KEY = "libra.preprocess.common.kmerindex.kmerindextable";
    
    private String name;
    private List<KmerIndexTableRecord> records = new ArrayList<KmerIndexTableRecord>();
    
    public static KmerIndexTable createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerIndexTable) serializer.fromJsonFile(file, KmerIndexTable.class);
    }
    
    public static KmerIndexTable createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerIndexTable) serializer.fromJson(json, KmerIndexTable.class);
    }
    
    public static KmerIndexTable createInstance(Configuration conf) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerIndexTable) serializer.fromJsonConfiguration(conf, HADOOP_CONFIG_KEY, KmerIndexTable.class);
    }
    
    public static KmerIndexTable createInstance(FileSystem fs, Path file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerIndexTable) serializer.fromJsonFile(fs, file, KmerIndexTable.class);
    }
    
    public KmerIndexTable() {
    }
    
    public KmerIndexTable(String name) {
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
    public void addRecord(KmerIndexTableRecord record) {
        this.records.add(record);
        Collections.sort(this.records);
    }
    
    @JsonProperty("record")
    public void addRecord(Collection<KmerIndexTableRecord> record) {
        this.records.addAll(record);
        Collections.sort(this.records);
    }
    
    @JsonProperty("record")
    public Collection<KmerIndexTableRecord> getRecord() {
        return this.records;
    }
    
    @JsonIgnore
    public int getSize() {
        return this.records.size();
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
