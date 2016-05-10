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

import libra.preprocess.common.helpers.KmerIndexHelper;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import libra.common.hadoop.io.datatypes.CompressedSequenceWritable;
import libra.common.hadoop.io.reader.map.IndexCloseableMapFileReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;

/**
 *
 * @author iychoi
 */
public class KmerIndexReader extends AKmerIndexReader {
    
    private static final Log LOG = LogFactory.getLog(KmerIndexReader.class);
    
    private static final int BUFFER_SIZE = 100;
    
    private FileSystem fs;
    private Path indexPath;
    private Path[] indexPartFilePaths;
    private Configuration conf;
    private IndexCloseableMapFileReader[] mapfileReaders;
    private CompressedSequenceWritable beginKey;
    private CompressedSequenceWritable endKey;
    private BlockingQueue<KmerIndexBufferEntry> buffer = new LinkedBlockingQueue<KmerIndexBufferEntry>();
    private String[] chunkLastKeys;
    private boolean eof;
    
    private int currentIndex;
    
    public KmerIndexReader(FileSystem fs, Path kmerIndexPath, Configuration conf) throws IOException {
        initialize(fs, kmerIndexPath, null, null, conf);
    }
    
    public KmerIndexReader(FileSystem fs, Path kmerIndexPath, CompressedSequenceWritable beginKey, CompressedSequenceWritable endKey, Configuration conf) throws IOException {
        initialize(fs, kmerIndexPath, beginKey, endKey, conf);
    }
    
    public KmerIndexReader(FileSystem fs, Path kmerIndexPath, String beginKey, String endKey, Configuration conf) throws IOException {
        initialize(fs, kmerIndexPath, new CompressedSequenceWritable(beginKey), new CompressedSequenceWritable(endKey), conf);
    }
    
    private Path[] reorderIndexParts(Path[] indexPaths) {
        int len = indexPaths.length;
        Path[] orderedArr = new Path[len];
        
        for(int i=0;i<len;i++) {
            int part = KmerIndexHelper.getIndexPartID(indexPaths[i]);
            if(part < 0 || part >= len) {
                return null;
            } else {
                orderedArr[part] = indexPaths[i];
            }
        }
        
        for(int i=0;i<len;i++) {
            if(orderedArr[i] == null) {
                return null;
            }
        }
        
        return orderedArr;
    }
    
    private void initialize(FileSystem fs, Path kmerIndexPath, CompressedSequenceWritable beginKey, CompressedSequenceWritable endKey, Configuration conf) throws IOException {
        this.fs = fs;
        this.conf = conf;
        this.beginKey = beginKey;
        this.endKey = endKey;
        this.indexPath = kmerIndexPath;

        Path[] kmerIndexPartFilePath = null;
        kmerIndexPartFilePath = KmerIndexHelper.getKmerIndexPartFilePath(conf, this.indexPath);
        
        this.indexPartFilePaths = reorderIndexParts(kmerIndexPartFilePath);
        if(this.indexPartFilePaths == null) {
            throw new IOException("part of index is missing");
        }
        
        this.mapfileReaders = new IndexCloseableMapFileReader[this.indexPartFilePaths.length];

        KmerIndexIndex indexIndex = KmerIndexIndex.createInstance(fs, this.indexPath);
        
        this.chunkLastKeys = indexIndex.getSortedLastKeys().toArray(new String[0]);
        
        if(this.chunkLastKeys.length != this.indexPartFilePaths.length) {
            throw new IOException("KmerIndexChunkKeys length is different from given index group length");
        }
        
        this.currentIndex = 0;
        if(beginKey != null) {
            boolean bFound = false;
            for(int i=0;i<this.chunkLastKeys.length;i++) {
                if(this.chunkLastKeys[i].compareToIgnoreCase(beginKey.getSequence()) >= 0) {
                    //found
                    this.currentIndex = i;
                    bFound = true;
                    break;
                }
            }
            
            if(!bFound) {
                throw new IOException("Could not find start point from kmer index");
            }
        }
        
        this.mapfileReaders[this.currentIndex] = new IndexCloseableMapFileReader(fs, this.indexPartFilePaths[this.currentIndex].toString(), conf);
        if(beginKey != null) {
            this.eof = false;
            seek(beginKey);
        } else {
            this.eof = false;
            fillBuffer();
        }
    }
    
    private void fillBuffer() throws IOException {
        if(!this.eof) {
            CompressedSequenceWritable lastBufferedKey = null;
            int added = 0;
            while(added < BUFFER_SIZE) {
                CompressedSequenceWritable key = new CompressedSequenceWritable();
                IntWritable val = new IntWritable();
                if(this.mapfileReaders[this.currentIndex].next(key, val)) {
                    KmerIndexBufferEntry entry = new KmerIndexBufferEntry(key, val);
                    if(!this.buffer.offer(entry)) {
                        throw new IOException("buffer is full");
                    }
                    
                    lastBufferedKey = key;
                    added++;
                } else {
                    // EOF of this part
                    this.mapfileReaders[this.currentIndex].close();
                    this.mapfileReaders[this.currentIndex] = null;
                    this.currentIndex++;
                    
                    if(this.currentIndex == this.mapfileReaders.length) {
                        // last
                        this.eof = true;
                        break;
                    } else {
                        this.mapfileReaders[this.currentIndex] = new IndexCloseableMapFileReader(this.fs, this.indexPartFilePaths[this.currentIndex].toString(), this.conf);
                        this.mapfileReaders[this.currentIndex].closeIndex();
                    }
                }
            }
            
            if(this.endKey != null && lastBufferedKey != null) {
                if(lastBufferedKey.compareTo(this.endKey) > 0) {
                    // recheck buffer
                    BlockingQueue<KmerIndexBufferEntry> new_buffer = new LinkedBlockingQueue<KmerIndexBufferEntry>();

                    KmerIndexBufferEntry entry = this.buffer.poll();
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
        if(this.mapfileReaders != null) {
            for(int i=0;i<this.mapfileReaders.length;i++) {
                if(this.mapfileReaders[i] != null) {
                    this.mapfileReaders[i].close();
                    this.mapfileReaders[i] = null;
                }
            }
            this.mapfileReaders = null;
        }
    }
    
    @Override
    public Path getIndexPath() {
        return this.indexPath;
    }
    
    private void seek(CompressedSequenceWritable key) throws IOException {
        this.buffer.clear();
        
        IntWritable val = new IntWritable();
        CompressedSequenceWritable nextKey = (CompressedSequenceWritable)this.mapfileReaders[this.currentIndex].getClosest(key, val);
        if(nextKey == null) {
            this.eof = true;
        } else {
            this.eof = false;
            
            if(this.endKey != null) {
                if(nextKey.compareTo(this.endKey) <= 0) {
                    KmerIndexBufferEntry entry = new KmerIndexBufferEntry(nextKey, val);
                    if(!this.buffer.offer(entry)) {
                        throw new IOException("buffer is full");
                    }

                    fillBuffer();
                } else {
                    this.eof = true;
                }
            } else {
                KmerIndexBufferEntry entry = new KmerIndexBufferEntry(nextKey, val);
                if(!this.buffer.offer(entry)) {
                    throw new IOException("buffer is full");
                }

                fillBuffer();
            }
        }
    }
    
    @Override
    public boolean next(CompressedSequenceWritable key, IntWritable val) throws IOException {
        KmerIndexBufferEntry entry = this.buffer.poll();
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
