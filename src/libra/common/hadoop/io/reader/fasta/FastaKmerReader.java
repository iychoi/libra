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
package libra.common.hadoop.io.reader.fasta;

import java.io.IOException;
import libra.common.hadoop.io.format.fasta.FastaKmerInputFormat;
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
public class FastaKmerReader extends RecordReader<LongWritable, Text> {

    private static final Log LOG = LogFactory.getLog(FastaKmerReader.class);
    
    public static final char READ_DELIMITER = '>';
    
    private int kmersize = 0;
    private CompressionCodecFactory compressionCodecs = null;
    private long start;
    private long pos;
    private long end;
    private LineReader in;
    private int maxLineLength;
    private LongWritable key;
    private Text value;
    private Text buffer;
    private Text tempLine;
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
        this.kmersize = FastaKmerInputFormat.getKmerSize(conf);
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
        
        this.buffer = new Text();
        
        if(inTheMiddle) {
            // find new start line
            this.start += this.in.readLine(new Text(), 0, (int)Math.min((long)Integer.MAX_VALUE, this.end - this.start));
            
            // back off
            FSDataInputStream fileIn2 = fs.open(file);
            fileIn2.seek(this.start - 1000);
            
            LineReader in2 = new LineReader(fileIn2, conf);
            Text tempLine = new Text();
            long curpos = this.start - 1000;
            while(curpos < this.start) {
                curpos += in2.readLine(tempLine, this.maxLineLength, (int) (this.start - curpos));
                //LOG.info("Check prev-line - " + curpos + " / " + tempLine.toString());
            }
            
            if(tempLine.charAt(0) == READ_DELIMITER) {
                // clean start
                this.buffer.clear();
            } else {
                // leave k-1 seq in the buffer
                String seq = tempLine.toString().trim();
                if(seq.length() >= this.kmersize - 1) {
                    String left = seq.substring(seq.length() - this.kmersize + 1);
                    this.buffer.set(left);
                } else {
                    String bufferString = this.buffer.toString();
                    String newString = bufferString + seq;
                    if(newString.length() >= this.kmersize - 1) {
                        String left = newString.substring(newString.length() - this.kmersize + 1);
                        this.buffer.set(left);
                    } else {
                        this.buffer.set(newString);
                    }
                }
            }
            
            in2.close();
        }
        
        this.pos = this.start;
        
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
        
        int newSize = 0;
        if(this.tempLine == null) {
            this.tempLine = new Text();
        }
        this.tempLine.clear();
        while(this.pos < this.end) {
            newSize = this.in.readLine(this.tempLine, this.maxLineLength, (int) Math.max(Math.min(Integer.MAX_VALUE, this.end - this.pos), this.maxLineLength));
            //LOG.info("We read - " + this.tempLine.toString());
            if(newSize == 0) {
                // EOF
                break;
            }
            
            this.pos += newSize;
            
            if(this.tempLine.charAt(0) == READ_DELIMITER) {
                this.buffer.clear();
                this.tempLine.clear();
                // skip if it's header
            } else {
                if(newSize < this.maxLineLength) {
                    break;
                }
                // line too long
                LOG.info("Skipped line of size " + newSize + " at pos " + (this.pos - newSize));
            }
        }
        
        if(newSize == 0) {
            this.key = null;
            this.value = null;
            this.buffer = null;
            return false;
        } else {
            String bufferString = this.buffer.toString();
            String readString = this.tempLine.toString().trim();
            String newString = bufferString + readString;
            //LOG.info("Pass sequence to mapper - " + newString);
            this.value.set(newString);
            if(newString.length() > this.kmersize) {
                this.buffer.set(newString.substring(newString.length() - this.kmersize + 1));
            } else {
                this.buffer.clear();
            }
            return true;
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
