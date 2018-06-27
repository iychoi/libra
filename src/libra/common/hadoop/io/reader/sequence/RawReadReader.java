/*
 * Copyright 2018 iychoi.
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
package libra.common.hadoop.io.reader.sequence;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import libra.common.sequence.Read;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

/**
 *
 * @author iychoi
 */
public class RawReadReader implements Closeable {

    private static final Log LOG = LogFactory.getLog(RawReadReader.class);
    
    private SampleFormat format;
    private LineReader in;
    private Text buffer = new Text();
    private int bufferConsumed;
    private char delimiter;
    private boolean finished = false;

    public RawReadReader(SampleFormat format, InputStream in) {
        this.format = format;
        this.in = new LineReader(in);
        this.bufferConsumed = 0;
        this.finished = false;
        
        _setDelimiter();
    }
    
    public RawReadReader(SampleFormat format, InputStream in, int bufferSize) {
        this.format = format;
        this.in = new LineReader(in, bufferSize);
        this.bufferConsumed = 0;
        this.finished = false;
        
        _setDelimiter();
    }
    
    public RawReadReader(SampleFormat format, InputStream in, Configuration conf) throws IOException {
        this.format = format;
        this.in = new LineReader(in, conf);
        this.bufferConsumed = 0;
        this.finished = false;
        
        _setDelimiter();
    }

    @Override
    public void close() throws IOException {
        this.finished = true;
        this.in.close();
    }
    
    private void _setDelimiter() {
        switch(this.format) {
            case FASTA:
                this.delimiter = Read.FASTA_READ_DESCRIPTION_IDENTIFIER;
                break;
            case FASTQ:
                this.delimiter = Read.FASTQ_READ_DESCRIPTION_IDENTIFIER;
                break;
        }
    }
    
    private boolean _fillBuffer(boolean force) throws IOException {
        // returns hasData
        if(this.bufferConsumed == 0 || force) {
            // fill buffer
            this.bufferConsumed = this.in.readLine(this.buffer, Integer.MAX_VALUE, Integer.MAX_VALUE);
            if(this.bufferConsumed <= 0) {
                this.bufferConsumed = 0;
                return false;
            } else {
                return true;
            }
        } else {
            return true;
        }
    }

    public long skipIncompleteRead() throws IOException {
        if(this.finished) {
            return 0;
        }
        
        long bytesConsumed = 0;
        
        boolean hasBufferData = _fillBuffer(false);
        if(!hasBufferData) {
            //EOF
            this.finished = true;
            return bytesConsumed;
        }
        
        boolean headerFound = false;
        while(hasBufferData) {
            if(this.buffer.getLength() > 0 && this.buffer.charAt(0) == this.delimiter) {
                headerFound = true;
                break;
            } else {
                bytesConsumed += this.bufferConsumed;
                // refill buffer
                hasBufferData = _fillBuffer(true);
            }
        }
        
        if(!headerFound) {
            //EOF
            bytesConsumed += this.bufferConsumed;
            this.finished = true;
            this.bufferConsumed = 0;
        }
        
        return bytesConsumed;
    }
    
    public long readRead(Read read) throws IOException {
        read.clear();
        
        long bytesConsumed = 0;
        bytesConsumed += skipIncompleteRead();
        
        if(this.finished) {
            return bytesConsumed;
        }
        
        // check buffer has a header
        boolean hasBufferData = _fillBuffer(false);
        if(!hasBufferData) {
            //EOF
            this.finished = true;
            return bytesConsumed;
        }
        
        if(this.buffer.getLength() > 0 && this.buffer.charAt(0) == this.delimiter) {
            // GO!
            // add header
            List<String> lines = new ArrayList<String>();
        
            String lineStr = this.buffer.toString();
            if(lineStr.trim().length() > 0) {
                lines.add(lineStr);
            }
            
            bytesConsumed += this.bufferConsumed;
            hasBufferData = _fillBuffer(true);
            
            boolean nextHeaderFound = false;
            while(hasBufferData) {
                if(this.buffer.getLength() > 0 && this.buffer.charAt(0) == this.delimiter) {
                    nextHeaderFound = true;
                    break;
                } else {
                    lineStr = this.buffer.toString();
                    if(lineStr.trim().length() > 0) {
                        lines.add(lineStr);
                    }

                    bytesConsumed += this.bufferConsumed;
                    // refill buffer
                    hasBufferData = _fillBuffer(true);
                }
            }
            
            if(!nextHeaderFound) {
                //EOF
                this.finished = true;
            }
            
            read.parse(lines);
        } else {
            throw new IOException(String.format("Unknown data - %s", this.buffer.toString()));
        }
        
        return bytesConsumed;
    }
}
