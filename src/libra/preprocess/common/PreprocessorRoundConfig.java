/*
 * Copyright 2017 iychoi.
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
package libra.preprocess.common;

import java.io.File;
import java.io.IOException;
import libra.common.json.JsonSerializer;
import libra.preprocess.common.filetable.FileTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class PreprocessorRoundConfig extends PreprocessorConfig {
    private FileTable fileTable;
    
    public static PreprocessorRoundConfig createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (PreprocessorRoundConfig) serializer.fromJsonFile(file, PreprocessorRoundConfig.class);
    }
    
    public static PreprocessorRoundConfig createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (PreprocessorRoundConfig) serializer.fromJson(json, PreprocessorRoundConfig.class);
    }
    
    public static PreprocessorRoundConfig createInstance(Configuration conf) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (PreprocessorRoundConfig) serializer.fromJsonConfiguration(conf, HADOOP_CONFIG_KEY, PreprocessorRoundConfig.class);
    }
    
    public static PreprocessorRoundConfig createInstance(FileSystem fs, Path file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (PreprocessorRoundConfig) serializer.fromJsonFile(fs, file, PreprocessorRoundConfig.class);
    }
    
    public PreprocessorRoundConfig() {
        
    }
    
    public PreprocessorRoundConfig(PreprocessorConfig config) {
        super(config);
    }
    
    @JsonProperty("file_table")
    public FileTable getFileTable() {
        return this.fileTable;
    }
    
    @JsonProperty("file_table")
    public void setFileTable(FileTable fileTable) {
        this.fileTable = fileTable;
    }
}
