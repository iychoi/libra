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

import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class NamedOutputRecord implements Comparable<NamedOutputRecord> {
    private String identifier;
    private String filename;
    
    public NamedOutputRecord() {
    }
    
    public NamedOutputRecord(Path file) {
        initialize(NamedOutputs.getSafeIdentifier(file.getName()), file.getName());
    }
    
    public NamedOutputRecord(String identifier, Path file) {
        initialize(identifier, file.getName());
    }
    
    public NamedOutputRecord(String filename) {
        initialize(NamedOutputs.getSafeIdentifier(filename), filename);
    }
    
    public NamedOutputRecord(String identifier, String filename) {
        initialize(identifier, filename);
    }
    
    @JsonIgnore
    private void initialize(String identifier, String filename) {
        this.identifier = identifier;
        this.filename = filename;
    }
    
    @JsonProperty("identifier")
    public String getIdentifier() {
        return this.identifier;
    }
    
    @JsonProperty("identifier")
    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }
    
    @JsonProperty("filename")
    public String getFilename() {
        return this.filename;
    }
    
    @JsonProperty("filename")
    public void setFilename(String filename) {
        this.filename = filename;
    }
    
    @JsonIgnore
    @Override
    public int compareTo(NamedOutputRecord right) {
        return this.identifier.compareToIgnoreCase(right.identifier);
    }
}
