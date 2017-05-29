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
package libra.preprocess.common.kmerindex;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class KmerIndexTableRecord implements Comparable<KmerIndexTableRecord> {
    private String indexDataFile;
    private String lastKmer;
    
    public KmerIndexTableRecord() {
        
    }
    
    public KmerIndexTableRecord(String indexDataFile, String lastKmer) {
        this.indexDataFile = indexDataFile;
        this.lastKmer = lastKmer;
    }
    
    @JsonProperty("last_kmer")
    public String getLastKmer() {
        return this.lastKmer;
    }
    
    @JsonProperty("last_kmer")
    public void setLastKmer(String lastKmer) {
        this.lastKmer = lastKmer;
    }
    
    @JsonProperty("data_file")
    public String getIndexDataFile() {
        return this.indexDataFile;
    }
    
    @JsonProperty("data_file")
    public void setIndexDataFile(String indexDataFile) {
        this.indexDataFile = indexDataFile;
    }
    
    @JsonIgnore
    @Override
    public int compareTo(KmerIndexTableRecord right) {
        return this.lastKmer.compareToIgnoreCase(right.lastKmer);
    }
}
