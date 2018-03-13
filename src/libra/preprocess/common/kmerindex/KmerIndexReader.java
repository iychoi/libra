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

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import libra.common.hadoop.io.datatypes.IntArrayWritable;
import libra.common.hadoop.io.datatypes.CompressedSequenceWritable;
import libra.common.hadoop.io.reader.map.IndexCloseableMapFileReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author iychoi
 */
public class KmerIndexReader extends AKmerIndexReader {
    
    private static final Log LOG = LogFactory.getLog(KmerIndexReader.class);
    
    private static final int BUFFER_SIZE = 100;
    
    private Configuration conf;
    private FileSystem fs;
    private Path kmerIndexTablePath;
    private KmerIndexTable indexTable;
    private KmerIndexTableRecord[] tableRecords;
    
    private IndexCloseableMapFileReader[] indexDataReaders;
    private CompressedSequenceWritable beginKey;
    private CompressedSequenceWritable endKey;
    private BlockingQueue<KmerIndexRecordBufferEntry> buffer = new LinkedBlockingQueue<KmerIndexRecordBufferEntry>();
    private boolean eof;
    
    private int currentIndexDataID;
    
    public KmerIndexReader(FileSystem fs, Path kmerIndexTablePath, Configuration conf) throws IOException {
        initialize(fs, kmerIndexTablePath, null, null, conf);
    }
    
    public KmerIndexReader(FileSystem fs, Path kmerIndexTablePath, CompressedSequenceWritable beginKey, CompressedSequenceWritable endKey, Configuration conf) throws IOException {
        initialize(fs, kmerIndexTablePath, beginKey, endKey, conf);
    }
    
    public KmerIndexReader(FileSystem fs, Path kmerIndexTablePath, String beginKey, String endKey, Configuration conf) throws IOException {
        initialize(fs, kmerIndexTablePath, new CompressedSequenceWritable(beginKey), new CompressedSequenceWritable(endKey), conf);
    }
    
    private void initialize(FileSystem fs, Path kmerIndexTablePath, CompressedSequenceWritable beginKey, CompressedSequenceWritable endKey, Configuration conf) throws IOException {
        this.conf = conf;
        this.fs = fs;
        this.beginKey = beginKey;
        this.endKey = endKey;
        this.kmerIndexTablePath = kmerIndexTablePath;
        this.indexTable = KmerIndexTable.createInstance(fs, this.kmerIndexTablePath);
        this.tableRecords = this.indexTable.getRecord().toArray(new KmerIndexTableRecord[0]);
        this.indexDataReaders = new IndexCloseableMapFileReader[this.tableRecords.length];
        
        this.currentIndexDataID = 0;
        boolean bFound = false;
        if(beginKey != null) {
            for(int i=0;i<this.tableRecords.length;i++) {
                this.currentIndexDataID = i;
                if(this.tableRecords[i].getLastKmer().compareToIgnoreCase(beginKey.getSequence()) >= 0) {
                    //found
                    bFound = true;
                    break;
                }
            }
            
            //if(!bFound) {
            //    throw new IOException(String.format("Could not find start point from kmer index - %s in %s", beginKey.getSequence(), kmerIndexTablePath.toString()));
            //}
        } else {
            bFound = true;
        }
        
        if(bFound) {
            Path indexDataFile = new Path(this.kmerIndexTablePath.getParent(), this.tableRecords[this.currentIndexDataID].getIndexDataFile());
            this.indexDataReaders[this.currentIndexDataID] = new IndexCloseableMapFileReader(fs, indexDataFile.toString(), conf);
            if(beginKey != null) {
                this.eof = false;
                seek(beginKey);
            } else {
                this.eof = false;
                fillBuffer();
            }
        } else{
            this.eof = true;
            this.indexDataReaders[this.currentIndexDataID] = null;
        }
    }
    
    private void fillBuffer() throws IOException {
        if(!this.eof) {
            CompressedSequenceWritable lastBufferedKey = null;
            int added = 0;
            while(added < BUFFER_SIZE) {
                CompressedSequenceWritable key = new CompressedSequenceWritable();
                IntArrayWritable val = new IntArrayWritable();
                if(this.indexDataReaders[this.currentIndexDataID].next(key, val)) {
                    KmerIndexRecordBufferEntry entry = new KmerIndexRecordBufferEntry(key, val);
                    if(!this.buffer.offer(entry)) {
                        throw new IOException("buffer is full");
                    }
                    
                    lastBufferedKey = key;
                    added++;
                } else {
                    // EOF of this part
                    this.indexDataReaders[this.currentIndexDataID].close();
                    this.indexDataReaders[this.currentIndexDataID] = null;
                    this.currentIndexDataID++;
                    
                    if(this.currentIndexDataID == this.indexDataReaders.length) {
                        // last
                        this.eof = true;
                        break;
                    } else {
                        Path indexDataFile = new Path(this.kmerIndexTablePath.getParent(), this.tableRecords[this.currentIndexDataID].getIndexDataFile());
                        this.indexDataReaders[this.currentIndexDataID] = new IndexCloseableMapFileReader(this.fs, indexDataFile.toString(), this.conf);
                        this.indexDataReaders[this.currentIndexDataID].closeIndex();
                    }
                }
            }
            
            if(this.endKey != null && lastBufferedKey != null) {
                if(lastBufferedKey.compareTo(this.endKey) > 0) {
                    // recheck buffer
                    BlockingQueue<KmerIndexRecordBufferEntry> new_buffer = new LinkedBlockingQueue<KmerIndexRecordBufferEntry>();

                    KmerIndexRecordBufferEntry entry = this.buffer.poll();
                    while(entry != null) {
                        if(entry.getKey().compareTo(this.endKey) <= 0) {
                            if(!new_buffer.offer(entry)) {
                                throw new IOException("buffer is full");
                            }
                        }

                        entry = this.buffer.poll();
                    }

                    this.buffer = new_buffer;
                    this.eof = true;
                }
            }
        }
    }
    
    @Override
    public void close() throws IOException {
        if(this.indexDataReaders != null) {
            for(int i=0;i<this.indexDataReaders.length;i++) {
                if(this.indexDataReaders[i] != null) {
                    this.indexDataReaders[i].close();
                    this.indexDataReaders[i] = null;
                }
            }
            this.indexDataReaders = null;
        }
    }
    
    @Override
    public Path getKmerIndexTablePath() {
        return this.kmerIndexTablePath;
    }
    
    private void seek(CompressedSequenceWritable key) throws IOException {
        this.buffer.clear();
        
        IntArrayWritable val = new IntArrayWritable();
        CompressedSequenceWritable nextKey = (CompressedSequenceWritable)this.indexDataReaders[this.currentIndexDataID].getClosest(key, val);
        if(nextKey == null) {
            this.eof = true;
        } else {
            this.eof = false;
            
            if(this.endKey != null) {
                if(nextKey.compareTo(this.endKey) <= 0) {
                    KmerIndexRecordBufferEntry entry = new KmerIndexRecordBufferEntry(nextKey, val);
                    if(!this.buffer.offer(entry)) {
                        throw new IOException("buffer is full");
                    }

                    fillBuffer();
                } else {
                    this.eof = true;
                }
            } else {
                KmerIndexRecordBufferEntry entry = new KmerIndexRecordBufferEntry(nextKey, val);
                if(!this.buffer.offer(entry)) {
                    throw new IOException("buffer is full");
                }

                fillBuffer();
            }
        }
    }
    
    @Override
    public boolean next(CompressedSequenceWritable key, IntArrayWritable val) throws IOException {
        KmerIndexRecordBufferEntry entry = this.buffer.poll();
        if(entry != null) {
            key.set(entry.getKey());
            val.set(entry.getVal().get());
            return true;
        }
        
        fillBuffer();
        entry = this.buffer.poll();
        if(entry != null) {
            key.set(entry.getKey());
            val.set(entry.getVal().get());
            return true;
        }
        return false;
    }
}
