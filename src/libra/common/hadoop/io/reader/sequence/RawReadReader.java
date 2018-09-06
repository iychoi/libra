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
    
    private static final int LINE_BUFFERS = 4;
    
    private SampleFormat format;
    private LineReader in;
    private Text[] buffers = new Text[LINE_BUFFERS];
    private int[] bufferConsumed = new int[LINE_BUFFERS];
    private boolean eof = false;
    private boolean finished = false;

    public RawReadReader(SampleFormat format, InputStream in) {
        this.format = format;
        this.in = new LineReader(in);
        for(int i=0;i<LINE_BUFFERS;i++) {
            this.buffers[i] = new Text();
            this.bufferConsumed[i] = 0;
        }
        this.eof = false;
        this.finished = false;
    }
    
    public RawReadReader(SampleFormat format, InputStream in, int bufferSize) {
        this.format = format;
        this.in = new LineReader(in, bufferSize);
        for(int i=0;i<LINE_BUFFERS;i++) {
            this.buffers[i] = new Text();
            this.bufferConsumed[i] = 0;
        }
        this.eof = false;
        this.finished = false;
    }
    
    public RawReadReader(SampleFormat format, InputStream in, Configuration conf) throws IOException {
        this.format = format;
        this.in = new LineReader(in, conf);
        for(int i=0;i<LINE_BUFFERS;i++) {
            this.buffers[i] = new Text();
            this.bufferConsumed[i] = 0;
        }
        this.eof = false;
        this.finished = false;
    }

    @Override
    public void close() throws IOException {
        this.eof = true;
        this.finished = true;
        this.in.close();
    }
    
    private void _shiftBuffer(int steps) {
        if(steps <= 0) {
            return;
        }
        
        if(steps >= LINE_BUFFERS) {
            for(int i=0;i<LINE_BUFFERS;i++) {
                this.buffers[i].clear();
                this.bufferConsumed[i] = 0;
            }
        } else {
            for(int i=0;i<LINE_BUFFERS;i++) {
                if(i+steps < LINE_BUFFERS) {
                    Text temp = this.buffers[i];
                    this.buffers[i] = this.buffers[i+steps];
                    this.buffers[i+steps] = temp;
                    this.buffers[i+steps].clear();
                    this.bufferConsumed[i] = this.bufferConsumed[i+steps];
                }
            }
            
            for(int i=0;i<steps;i++) {
                int idx = LINE_BUFFERS-i-1;
                this.buffers[idx].clear();
                this.bufferConsumed[idx] = 0;
            }
        }
    }
    
    private int _countEmptyBuffer() {
        int count = 0;
        for(int i=0;i<LINE_BUFFERS;i++) {
            int idx = LINE_BUFFERS-i-1;
            if(this.bufferConsumed[idx] == 0) {
                count++;
            } else {
                break;
            }
        }
        return count;
    }
    
    private boolean _fillBuffer() throws IOException {
        // returns hasData
        int emptyBuffers = _countEmptyBuffer();
        if(this.eof) {
            if(emptyBuffers == LINE_BUFFERS) {
                return false;
            } else {
                return true;
            }
        }
        
        if(emptyBuffers > 0) {
            // fill buffer
            int filled = 0;
            for(int i=0;i<emptyBuffers;i++) {
                int idx = LINE_BUFFERS-emptyBuffers+i;
                this.bufferConsumed[idx] = this.in.readLine(this.buffers[idx], Integer.MAX_VALUE, Integer.MAX_VALUE);
                if(this.bufferConsumed[idx] <= 0) {
                    this.buffers[idx].clear();
                    this.bufferConsumed[idx] = 0;
                    this.eof = true;
                    break;
                } else {
                    filled++;
                }
            }
            
            if(LINE_BUFFERS - emptyBuffers + filled > 0) {
                return true;
            } else {
                return false;
            }
        } else {
            return true;
        }
    }
    
    private long _skipIncompleteFASTARead() throws IOException {
        if(this.finished) {
            return 0;
        }
        
        boolean hasBufferData = _fillBuffer();
        if(!hasBufferData) {
            //EOF
            this.finished = true;
            return 0;
        }
        
        long bytesConsumed = 0;
        boolean headerFound = false;
        while(hasBufferData) {
            if(this.buffers[0].getLength() > 0 && this.buffers[0].charAt(0) == Read.FASTA_READ_DESCRIPTION_IDENTIFIER) {
                headerFound = true;
                break;
            } else {
                bytesConsumed += this.bufferConsumed[0];
                // refill buffer
                _shiftBuffer(1);
                hasBufferData = _fillBuffer();
            }
        }
        
        if(!headerFound) {
            //EOF
            this.finished = true;
        }
        
        return bytesConsumed;
    }

    private long _skipIncompleteFASTQRead() throws IOException {
        if(this.finished) {
            return 0;
        }
        
        boolean hasBufferData = _fillBuffer();
        if(!hasBufferData) {
            //EOF
            this.finished = true;
            return 0;
        }
        
        long bytesConsumed = 0;
        boolean headerFound = false;
        while(hasBufferData) {
            int emptyBufferCount = _countEmptyBuffer();
            if(this.buffers[0].getLength() > 0 && this.buffers[0].charAt(0) == Read.FASTQ_READ_DESCRIPTION_IDENTIFIER &&
                    this.buffers[2].getLength() > 0 && this.buffers[2].charAt(0) == Read.FASTQ_READ_DESCRIPTION2_IDENTIFIER && 
                    emptyBufferCount == 0) {
                headerFound = true;
                break;
            } else {
                bytesConsumed += this.bufferConsumed[0];
                // refill buffer
                _shiftBuffer(1);
                hasBufferData = _fillBuffer();
            }
        }
        
        if(!headerFound) {
            //EOF
            this.finished = true;
        }
        
        return bytesConsumed;
    }
    
    public long skipIncompleteRead() throws IOException {
        switch(this.format) {
            case FASTA:
                return _skipIncompleteFASTARead();
            case FASTQ:
                return _skipIncompleteFASTQRead();
            default:
                throw new IOException("Unknown format");
        }
    }
    
    private void _printBuffer() {
        for(int i=0;i<LINE_BUFFERS;i++) {
            LOG.info(String.format("BUFFER[%d] = %s (%d)", i, this.buffers[i].toString(), this.bufferConsumed[i]));
        }
    }
    
    private long _readFASTARead(Read read) throws IOException {
        read.clear();
        
        long bytesConsumed = 0;
        bytesConsumed += skipIncompleteRead();
        
        if(this.finished) {
            return bytesConsumed;
        }
        
        // check buffer has a header
        boolean hasBufferData = _fillBuffer();
        if(!hasBufferData) {
            //EOF
            this.finished = true;
            return bytesConsumed;
        }
        
        if(this.buffers[0].getLength() > 0 && this.buffers[0].charAt(0) == Read.FASTA_READ_DESCRIPTION_IDENTIFIER) {
            // GO!
            // add header
            List<String> lines = new ArrayList<String>();
        
            String lineStr = this.buffers[0].toString();
            if(lineStr.trim().length() > 0) {
                lines.add(lineStr);
            }
            
            bytesConsumed += this.bufferConsumed[0];
            _shiftBuffer(1);
            hasBufferData = _fillBuffer();
            
            boolean nextHeaderFound = false;
            while(hasBufferData) {
                if(this.buffers[0].getLength() > 0 && this.buffers[0].charAt(0) == Read.FASTA_READ_DESCRIPTION_IDENTIFIER) {
                    nextHeaderFound = true;
                    break;
                } else {
                    lineStr = this.buffers[0].toString();
                    if(lineStr.trim().length() > 0) {
                        lines.add(lineStr);
                    }

                    bytesConsumed += this.bufferConsumed[0];
                    // refill buffer
                    _shiftBuffer(1);
                    hasBufferData = _fillBuffer();
                }
            }
            
            if(!nextHeaderFound) {
                //EOF
                this.finished = true;
            }
            
            read.parse(lines);
        } else {
            throw new IOException(String.format("Unknown data for FASTA read - %s", this.buffers[0].toString()));
        }
        
        return bytesConsumed;
    }
    
    private long _readFASTQRead(Read read) throws IOException {
        read.clear();
        
        long bytesConsumed = 0;
        bytesConsumed += skipIncompleteRead();
        
        if(this.finished) {
            return bytesConsumed;
        }
        
        // check buffer has a header
        boolean hasBufferData = _fillBuffer();
        if(!hasBufferData) {
            //EOF
            this.finished = true;
            return bytesConsumed;
        }
        
        int emptyBufferCount = _countEmptyBuffer();
        if(this.buffers[0].getLength() > 0 && this.buffers[0].charAt(0) == Read.FASTQ_READ_DESCRIPTION_IDENTIFIER &&
                this.buffers[2].getLength() > 0 && this.buffers[2].charAt(0) == Read.FASTQ_READ_DESCRIPTION2_IDENTIFIER &&
                emptyBufferCount == 0) {
            // GO!
            // add header
            List<String> lines = new ArrayList<String>();
        
            String lineStr = this.buffers[0].toString();
            if(lineStr.trim().length() > 0) {
                lines.add(lineStr);
            }
            
            bytesConsumed += this.bufferConsumed[0];
            _shiftBuffer(1);
            hasBufferData = _fillBuffer();
            
            boolean nextHeaderFound = false;
            while(hasBufferData) {
                emptyBufferCount = _countEmptyBuffer();
                //_printBuffer();
                
                if(this.buffers[0].getLength() > 0 && this.buffers[0].charAt(0) == Read.FASTQ_READ_DESCRIPTION_IDENTIFIER &&
                        this.buffers[2].getLength() > 0 && this.buffers[2].charAt(0) == Read.FASTQ_READ_DESCRIPTION2_IDENTIFIER &&
                        emptyBufferCount == 0) {
                    nextHeaderFound = true;
                    break;
                } else {
                    lineStr = this.buffers[0].toString();
                    if(lineStr.trim().length() > 0) {
                        lines.add(lineStr);
                    }

                    bytesConsumed += this.bufferConsumed[0];
                    // refill buffer
                    _shiftBuffer(1);
                    hasBufferData = _fillBuffer();
                }
            }
            
            if(!nextHeaderFound) {
                //EOF
                this.finished = true;
            }
            
            read.parse(lines);
        } else {
            throw new IOException(String.format("Unknown data for FASTQ read - %s", this.buffers[0].toString()));
        }
        
        return bytesConsumed;
    }
    
    public long readRead(Read read) throws IOException {
        switch(this.format) {
            case FASTA:
                return _readFASTARead(read);
            case FASTQ:
                return _readFASTQRead(read);
            default:
                throw new IOException("Unknown format");
        }
    }
}
