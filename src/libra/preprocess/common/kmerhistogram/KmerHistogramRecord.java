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
package libra.preprocess.common.kmerhistogram;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class KmerHistogramRecord implements Comparable<KmerHistogramRecord> {
    private String kmer;
    private long frequency;
    
    public KmerHistogramRecord() {
        
    }
    
    public KmerHistogramRecord(String key, long freqeuncy) {
        this.kmer = key;
        this.frequency = freqeuncy;
    }
    
    @JsonProperty("kmer")
    public String getKmer() {
        return this.kmer;
    }
    
    @JsonProperty("kmer")
    public void setKmer(String kmer) {
        this.kmer = kmer;
    }
    
    @JsonProperty("frequency")
    public long getFrequency() {
        return this.frequency;
    }
    
    @JsonProperty("frequency")
    public void setFrequency(long frequency) {
        this.frequency = frequency;
    }
    
    @JsonIgnore
    public void increaseFrequency(long frequency) {
        this.frequency += frequency;
    }
    
    @JsonIgnore
    public void increaseFrequency() {
        this.frequency++;
    }

    @JsonIgnore
    @Override
    public int compareTo(KmerHistogramRecord right) {
        return this.kmer.compareToIgnoreCase(right.kmer);
    }
}
