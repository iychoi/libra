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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Hashtable;
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
public class KmerMatchFileMapping {
    
    private static final Log LOG = LogFactory.getLog(KmerMatchFileMapping.class);
    
    private static final String HADOOP_CONFIG_KEY = "libra.common.kmermatch.kmermatcherfilemapping";
    
    private Hashtable<String, Integer> idTable;
    private List<String> objList;

    public static KmerMatchFileMapping createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerMatchFileMapping) serializer.fromJsonFile(file, KmerMatchFileMapping.class);
    }
    
    public static KmerMatchFileMapping createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerMatchFileMapping) serializer.fromJson(json, KmerMatchFileMapping.class);
    }
    
    public static KmerMatchFileMapping createInstance(Configuration conf) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerMatchFileMapping) serializer.fromJsonConfiguration(conf, HADOOP_CONFIG_KEY, KmerMatchFileMapping.class);
    }
    
    public static KmerMatchFileMapping createInstance(FileSystem fs, Path file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerMatchFileMapping) serializer.fromJsonFile(fs, file, KmerMatchFileMapping.class);
    }
    
    public KmerMatchFileMapping() {
        this.idTable = new Hashtable<String, Integer>();
        this.objList = new ArrayList<String>();
    }
    
    @JsonProperty("sample_files")
    public Collection<String> getSampleFiles() {
        return this.objList;
    }
    
    @JsonProperty("sample_files")
    public void addSampleFile(Collection<String> sampleFiles) {
        for(String sampleFile : sampleFiles) {
            addSampleFile(sampleFile);
        }
    }
    
    @JsonIgnore
    public void addSampleFile(String sampleFile) {
        this.idTable.put(sampleFile, this.objList.size());
        this.objList.add(sampleFile);
    }
    
    @JsonIgnore
    public int getIDFromSampleFile(String sampleFile) throws IOException {
        if(this.idTable.get(sampleFile) == null) {
            throw new IOException("could not find id from " + sampleFile);
        } else {
            return this.idTable.get(sampleFile);
        }
    }
    
    @JsonIgnore
    public String getSampleFileFromID(int id) throws IOException {
        if(this.objList.size() <= id) {
            throw new IOException("could not find a sample file from " + id);
        } else {
            return this.objList.get(id);
        }
    }
    
    @JsonIgnore
    public int getSize() {
        return this.objList.size();
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
