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
    private int partitionID;
    
    public KmerIndexTableRecord() {
        
    }
    
    public KmerIndexTableRecord(String indexDataFile, int partitionID) {
        this.indexDataFile = indexDataFile;
        this.partitionID = partitionID;
    }
    
    @JsonProperty("partition_id")
    public int getPartitionID() {
        return this.partitionID;
    }
    
    @JsonProperty("partition_id")
    public void setPartitionId(int partitionNo) {
        this.partitionID = partitionNo;
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
        Integer l = new Integer(this.partitionID);
        Integer r = new Integer(right.partitionID);
        
        return l.compareTo(r);
    }
}
