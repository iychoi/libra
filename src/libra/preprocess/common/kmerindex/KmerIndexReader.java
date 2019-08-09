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
    private int kmerSize;
    private Path kmerIndexTablePath;
    private int partitionNo;
    private KmerIndexTable indexTable;
    private KmerIndexTableRecord tableRecord;
    
    private IndexCloseableMapFileReader indexDataReader;
    private BlockingQueue<KmerIndexRecordBufferEntry> buffer = new LinkedBlockingQueue<KmerIndexRecordBufferEntry>();
    private boolean eof;
    
    public KmerIndexReader(FileSystem fs, int kmerSize, Path kmerIndexTablePath, int partitionNo, Configuration conf) throws IOException {
        initialize(fs, kmerSize, kmerIndexTablePath, partitionNo, conf);
    }
    
    private void initialize(FileSystem fs, int kmerSize, Path kmerIndexTablePath, int partitionNo, Configuration conf) throws IOException {
        this.conf = conf;
        this.fs = fs;
        this.kmerSize = kmerSize;
        this.kmerIndexTablePath = kmerIndexTablePath;
        this.partitionNo = partitionNo;
        this.indexTable = KmerIndexTable.createInstance(fs, this.kmerIndexTablePath);
        
        this.tableRecord = this.indexTable.getRecord(this.partitionNo);
        
        Path indexDataFile = new Path(this.kmerIndexTablePath.getParent(), this.tableRecord.getIndexDataFile());
        this.indexDataReader = new IndexCloseableMapFileReader(fs, indexDataFile.toString(), conf);
        
        this.eof = false;
        fillBuffer();
    }
    
    private void fillBuffer() throws IOException {
        if(!this.eof) {
            int added = 0;
            while(added < BUFFER_SIZE) {
                CompressedSequenceWritable key = new CompressedSequenceWritable();
                IntArrayWritable val = new IntArrayWritable();
                if(this.indexDataReader.next(key, val)) {
                    KmerIndexRecordBufferEntry entry = new KmerIndexRecordBufferEntry(key, val);
                    if(!this.buffer.offer(entry)) {
                        throw new IOException("buffer is full");
                    }
                    
                    added++;
                } else {
                    // EOF of this part
                    this.eof = true;
                    break;
                }
            }
        }
    }
    
    @Override
    public void close() throws IOException {
        if(this.indexDataReader != null) {
            this.indexDataReader.close();
            this.indexDataReader = null;
        }
    }
    
    @Override
    public Path getKmerIndexTablePath() {
        return this.kmerIndexTablePath;
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
