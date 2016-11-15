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
package libra.common.hadoop.io.reader.sequence;

import java.io.IOException;
import libra.common.hadoop.io.format.sequence.SequenceKmerInputFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

/**
 *
 * @author iychoi
 */
public class FastqKmerReader extends RecordReader<LongWritable, Text> {

    private static final Log LOG = LogFactory.getLog(FastqKmerReader.class);
    
    public static final char READ_DELIMITER = '@';
    public static final char READ_DELIMITER_P = '+';
    
    private int kmersize = 0;
    private CompressionCodecFactory compressionCodecs = null;
    private long start;
    private long pos;
    private long end;
    private LineReader in;
    private int maxLineLength;
    private LongWritable key;
    private Text value;
    private Text lastLine;
    private boolean isCompressed;
    private long uncompressedSize;
    
    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return this.key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return this.value;
    }

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context)
            throws IOException, InterruptedException {
        FileSplit split = (FileSplit) genericSplit;
        Configuration conf = context.getConfiguration();
        this.kmersize = SequenceKmerInputFormat.getKmerSize(conf);
        this.maxLineLength = conf.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
        this.start = split.getStart();
        this.end = this.start + split.getLength();
        final Path file = split.getPath();
        this.compressionCodecs = new CompressionCodecFactory(conf);
        final CompressionCodec codec = this.compressionCodecs.getCodec(file);
        
        // open the file and seek to the start of the split
        FileSystem fs = file.getFileSystem(conf);
        
        // get uncompressed length
        if (codec instanceof GzipCodec) {
            this.isCompressed = true;
            
            FSDataInputStream fileInCheckSize = fs.open(file);
            byte[] len = new byte[4];
            try {
                LOG.info("compressed input : " + file.getName());
                LOG.info("compressed file size : " + this.end);
                fileInCheckSize.skip(this.end - 4);
                IOUtils.readFully(fileInCheckSize, len, 0, len.length);
                this.uncompressedSize = (len[3] << 24) | (len[2] << 16) | (len[1] << 8) | len[0];
                if(this.uncompressedSize < 0) {
                    this.uncompressedSize = this.end;
                }
                LOG.info("uncompressed file size : " + this.uncompressedSize);
            } finally {
                fileInCheckSize.close();
            }
            
            this.end = Long.MAX_VALUE;
        } else if(codec != null) {
            this.isCompressed = true;
            this.end = Long.MAX_VALUE;
            this.uncompressedSize = Long.MAX_VALUE;
        } else {
            this.isCompressed = false;
        }
        
        // get inputstream
        FSDataInputStream fileIn = fs.open(file);
        boolean inTheMiddle = false;
        if (codec != null) {
            this.in = new LineReader(codec.createInputStream(fileIn), conf);
        } else {
            if (this.start != 0) {
                this.start--;
                fileIn.seek(this.start);
                
                inTheMiddle = true;
            }
            this.in = new LineReader(fileIn, conf);
        }
        
        if(inTheMiddle) {
            // find new start line
            this.start += this.in.readLine(new Text(), 0, (int)Math.min((long)Integer.MAX_VALUE, this.end - this.start));
        }
        
        this.lastLine = new Text();
        
        this.pos = this.start;
        
        int newSize = 0;
        boolean found = false;
        while(this.pos < this.end) {
            newSize = this.in.readLine(this.lastLine, this.maxLineLength, (int) Math.max(Math.min(Integer.MAX_VALUE, this.end - this.pos), this.maxLineLength));        
            if(newSize == 0) {
                // EOF
                break;
            }
            
            this.pos += newSize;
            
            if(this.lastLine.charAt(0) == READ_DELIMITER) {
                // we found!
                found = true;
                break;
            }
        }
        
        if(!found) {
            this.lastLine = null;
        }
        
        this.key = null;
        this.value = null;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (this.key == null) {
            this.key = new LongWritable();
        }
        this.key.set(this.pos);
        
        if(this.value == null) {
            this.value = new Text();
        }
        
        if(this.lastLine == null) {
            // no data
            return false;
        }
        
        if(this.lastLine.charAt(0) != READ_DELIMITER) {
            return false;
        }
        
        boolean found = false;
        for(int i=0;i<4;i++) {
            if(i == 3 && this.pos >= this.end) {
                // outside of a block
                this.lastLine = null;
                break;
            }
            
            int newSize = this.in.readLine(this.lastLine, this.maxLineLength, (int) Math.max(Math.min(Integer.MAX_VALUE, this.end - this.pos), this.maxLineLength));
            if(newSize == 0) {
                // EOF
                break;
            }

            if(i == 0) {
                this.value.set(this.lastLine);
                found = true;
            }
            
            this.pos += newSize;
        }
        
        if(found) {
            return true;
        } else {
            this.key = null;
            this.value = null;
            this.lastLine = null;
            return false;
        }
    }

    @Override
    public float getProgress() throws IOException {
        if(this.isCompressed) {
            if (this.start == this.uncompressedSize) {
                return 0.0f;
            } else {
                return Math.min(1.0f, (this.pos - this.start) / (float) (this.uncompressedSize - this.start));
            }
        } else {
            if (this.start == this.end) {
                return 0.0f;
            } else {
                return Math.min(1.0f, (this.pos - this.start) / (float) (this.end - this.start));
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (this.in != null) {
            this.in.close();
        }
    }
}
