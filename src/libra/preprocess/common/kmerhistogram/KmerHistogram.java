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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import libra.common.algorithms.CanonicalKmer;
import libra.common.helpers.SequenceHelper;
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
public class KmerHistogram {
    
    private static final Log LOG = LogFactory.getLog(KmerHistogram.class);
    
    private static final String HADOOP_CONFIG_KEY = "libra.preprocess.common.kmerhistogram.kmerhistogram";
    
    private static final int SAMPLING_CHARS = 6;
    
    private String name;
    private int kmerSize;
    
    private Hashtable<String, KmerHistogramRecord> recordCache = new Hashtable<String, KmerHistogramRecord>();
    private List<KmerHistogramRecord> recordList = new ArrayList<KmerHistogramRecord>();
    
    private long totalKmerCount = 0;
    private CanonicalKmer keySelectionAlg = new CanonicalKmer();
    
    public static KmerHistogram createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerHistogram) serializer.fromJsonFile(file, KmerHistogram.class);
    }
    
    public static KmerHistogram createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerHistogram) serializer.fromJson(json, KmerHistogram.class);
    }
    
    public static KmerHistogram createInstance(Configuration conf) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerHistogram) serializer.fromJsonConfiguration(conf, HADOOP_CONFIG_KEY, KmerHistogram.class);
    }
    
    public static KmerHistogram createInstance(FileSystem fs, Path file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerHistogram) serializer.fromJsonFile(fs, file, KmerHistogram.class);
    }
    
    public KmerHistogram() {
    }
    
    public KmerHistogram(String name, int kmerSize) {
        this.name = name;
        this.kmerSize = kmerSize;
    }
    
    @JsonProperty("name")
    public String getName() {
        return this.name;
    }
    
    @JsonProperty("name")
    public void setName(String name) {
        this.name = name;
    }
    
    @JsonProperty("kmer_size")
    public int getKmerSize() {
        return this.kmerSize;
    }
    
    @JsonProperty("kmer_size")
    public void setKmerSize(int kmerSize) {
        this.kmerSize = kmerSize;
    }
    
    @JsonIgnore
    public void takeSample(String sequence) {
        boolean pvalid = false;
        for (int i = 0; i < (sequence.length() - this.kmerSize + 1); i++) {
            // generate kmer
            String kmer = sequence.substring(i, i + this.kmerSize);
            if(pvalid) {
                if(!SequenceHelper.isValidSequence(kmer.charAt(this.kmerSize - 1))) {
                    //LOG.info("discard invalid kmer sequence : " + kmer);
                    pvalid = false;
                    continue;
                } else {
                    pvalid = true;
                }
            } else {
                if(!SequenceHelper.isValidSequence(kmer)) {
                    //LOG.info("discard invalid kmer sequence : " + kmer);
                    pvalid = false;
                    continue;
                } else {
                    pvalid = true;
                }
            }
            
            String selectedKey = this.keySelectionAlg.canonicalize(kmer);
            
            // take first N chars
            String selectedKeySample = selectedKey.substring(0, SAMPLING_CHARS);
            
            add(selectedKeySample);
        }
    }
    
    @JsonIgnore
    private void add(String kmer) {
        KmerHistogramRecord record = this.recordCache.get(kmer);
        if(record == null) {
            record = new KmerHistogramRecord(kmer, 1);
            this.recordCache.put(kmer, record);
            this.recordList.add(record);
        } else {
            record.increaseFrequency();
        }
        
        this.totalKmerCount++;
    }
    
    @JsonIgnore
    public long getTotalKmerCount() {
        return this.totalKmerCount;
    }
    
    @JsonIgnore
    public int getSamplingCharLen() {
        return SAMPLING_CHARS;
    }
    
    @JsonIgnore
    public Collection<KmerHistogramRecord> getRecord() {
        return this.recordList;
    }
    
    @JsonProperty("records")
    public Collection<KmerHistogramRecord> getSortedRecord() {
        Collections.sort(this.recordList, new KmerHistogramRecordComparator());
        return this.recordList;
    }
    
    @JsonProperty("records")
    public void addRecord(Collection<KmerHistogramRecord> records) {
        for(KmerHistogramRecord record : records) {
            addRecord(record);
        }
        
    }
    
    @JsonIgnore
    public void addRecord(KmerHistogramRecord record) {
        KmerHistogramRecord existingRecord = this.recordCache.get(record.getKmer());
        if(existingRecord == null) {
            this.recordCache.put(record.getKmer(), record);
            this.recordList.add(record);
        } else {
            existingRecord.increaseFrequency(record.getFrequency());
        }
        
        this.totalKmerCount += record.getFrequency();
    }
    
    @JsonIgnore
    public int getRecordNum() {
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
}
