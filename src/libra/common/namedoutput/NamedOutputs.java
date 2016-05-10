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
package libra.common.namedoutput;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import libra.common.json.JsonSerializer;
import libra.common.helpers.MapReduceHelper;
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
public class NamedOutputs {
    
    private static final Log LOG = LogFactory.getLog(NamedOutputs.class);
    
    private static final String HADOOP_CONFIG_KEY = "libra.common.namedoutput.namedoutputs";
    
    private Hashtable<String, Integer> identifierCache = new Hashtable<String, Integer>();
    private Hashtable<String, Integer> filenameCache = new Hashtable<String, Integer>();
    private List<NamedOutputRecord> recordList = new ArrayList<NamedOutputRecord>();
    
    public static NamedOutputs createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (NamedOutputs) serializer.fromJsonFile(file, NamedOutputs.class);
    }
    
    public static NamedOutputs createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (NamedOutputs) serializer.fromJson(json, NamedOutputs.class);
    }
    
    public static NamedOutputs createInstance(Configuration conf) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (NamedOutputs) serializer.fromJsonConfiguration(conf, HADOOP_CONFIG_KEY, NamedOutputs.class);
    }
    
    public static NamedOutputs createInstance(FileSystem fs, Path file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (NamedOutputs) serializer.fromJsonFile(fs, file, NamedOutputs.class);
    }
    
    public NamedOutputs() {
    }
    
    @JsonIgnore
    public void add(Path file) {
        add(file.getName());
    }
    
    @JsonIgnore
    public void add(Path[] files) {
        for(Path file : files) {
            add(file.getName());
        }
    }
    
    @JsonIgnore
    public void add(String[] filenames) {
        for(String filename : filenames) {
            add(filename);
        }
    }
    
    @JsonIgnore
    public void add(String filename) {
        String identifier = getSafeIdentifier(filename);
        
        if(this.identifierCache.get(identifier) == null) {
            try {
                // okey
                addRecord(identifier, filename);
            } catch (DuplicatedNamedOutputException ex) {
                LOG.error(ex);
            }
        } else {
            int trial = 0;
            boolean success = false;
            while(!success) {
                trial++;
                String identifierTrial = getSafeIdentifier(filename + trial);
                if(this.identifierCache.get(identifierTrial) == null) {
                    // okey
                    try {
                        addRecord(identifierTrial, filename);
                    } catch (DuplicatedNamedOutputException ex) {
                        LOG.error(ex);
                    }
                    success = true;
                    break;
                }
            }
        }
    }
    
    @JsonIgnore
    public NamedOutputRecord getRecord(String identifier) {
        Integer ret = this.identifierCache.get(identifier);
        if(ret == null) {
            return null;
        } else {
            return this.recordList.get(ret.intValue());
        }
    }
    
    @JsonIgnore
    public void addRecord(NamedOutputRecord record) throws DuplicatedNamedOutputException {
        addRecord(record.getIdentifier(), record.getFilename());
    }
    
    @JsonIgnore
    public void addRecord(String identifier, String filename) throws DuplicatedNamedOutputException {
        if(this.identifierCache.get(identifier) == null) {
            // okey
            NamedOutputRecord record = new NamedOutputRecord(identifier, filename);
            this.identifierCache.put(identifier, this.recordList.size());
            this.filenameCache.put(filename, this.recordList.size());
            this.recordList.add(record);
        } else {
            throw new DuplicatedNamedOutputException("duplicated named output record (" + identifier + ") is found");
        }
    }
    
    @JsonProperty("records")
    public Collection<NamedOutputRecord> getRecord() {
        return Collections.unmodifiableList(this.recordList);
    }
    
    @JsonProperty("records")
    public void addRecord(Collection<NamedOutputRecord> records) throws DuplicatedNamedOutputException {
        for(NamedOutputRecord record : records) {
            addRecord(record);
        }
    }
    
    @JsonIgnore
    public int getIDFromFilename(String filename) throws IOException {
        Integer ret = this.filenameCache.get(filename);
        if(ret == null) {
            throw new IOException("could not find id from " + filename);
        } else {
            return ret.intValue();
        }
    }
    
    @JsonIgnore
    public NamedOutputRecord getRecordFromID(int id) throws IOException {
        if(this.recordList.size() <= id) {
            throw new IOException("could not find record " + id);
        } else {
            return this.recordList.get(id);    
        }
    }
    
    @JsonIgnore
    public NamedOutputRecord getRecordFromMROutput(Path outputFile) throws IOException {
        return getRecordFromMROutput(outputFile.getName());
    }
    
    @JsonIgnore
    public NamedOutputRecord getRecordFromMROutput(String outputFilename) throws IOException {
        String identifier = MapReduceHelper.getOutputNameFromMapReduceOutput(outputFilename);
        Integer ret = this.identifierCache.get(identifier);
        if(ret == null) {
            throw new IOException("could not find record " + outputFilename);
        } else {
            return this.recordList.get(ret.intValue());
        }
    }
    
    @JsonIgnore
    public int getSize() {
        return this.recordList.size();
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
    
    @JsonIgnore
    public static String getSafeIdentifier(String filename) {
        StringBuffer sb = new StringBuffer();
        
        for (char ch : filename.toCharArray()) {
            boolean isSafe = false;
            if ((ch >= 'A') && (ch <= 'Z')) {
                isSafe = true;
            } else if ((ch >= 'a') && (ch <= 'z')) {
                isSafe = true;
            } else if ((ch >= '0') && (ch <= '9')) {
                isSafe = true;
            }
            
            if(isSafe) {
                sb.append(ch);
            }
        }
        
        return sb.toString();
    }
}
