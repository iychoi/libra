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

import java.io.IOException;
import libra.common.sequence.Read;
import libra.common.sequence.ReadInfo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 *
 * @author iychoi
 */
public class ReadRecordReader extends RecordReader<LongWritable, ReadInfo> {

    private static final Log LOG = LogFactory.getLog(ReadRecordReader.class);

    private long start;
    private long pos;
    private long end;
    private SplitReadReader in;
    private FSDataInputStream fileIn;
    private Seekable filePosition;
    private LongWritable key;
    private ReadInfo value;
    private Read rawValue;
    private boolean isCompressedInput;
    private Decompressor decompressor;
    private SampleFormat format;
    private String filename;
    
    public ReadRecordReader() {
    }

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
        FileSplit split = (FileSplit) genericSplit;
        Configuration conf = context.getConfiguration();
        this.start = split.getStart();
        this.end = this.start + split.getLength();
        final Path file = split.getPath();
        this.format = SampleFormat.fromPath(file);
        this.filename = file.getName();

        // open the file and seek to the start of the split
        final FileSystem fs = file.getFileSystem(conf);
        this.fileIn = fs.open(file);

        CompressionCodec codec = new CompressionCodecFactory(conf).getCodec(file);
        if (null != codec) {
            this.isCompressedInput = true;
            this.decompressor = CodecPool.getDecompressor(codec);
            
            if (codec instanceof SplittableCompressionCodec) {
                final SplitCompressionInputStream cIn = ((SplittableCompressionCodec) codec).createInputStream(
                                this.fileIn, this.decompressor, this.start, this.end,
                                SplittableCompressionCodec.READ_MODE.BYBLOCK);
                this.in = new CompressedSplitReadReader(this.format, cIn, conf);
                this.start = cIn.getAdjustedStart();
                this.end = cIn.getAdjustedEnd();
                this.filePosition = cIn;
            } else {
                this.in = new SplitReadReader(this.format, codec.createInputStream(this.fileIn, this.decompressor), conf);
                this.filePosition = this.fileIn;
            }
        } else {
            this.fileIn.seek(this.start);
            this.in = new UncompressedSplitReadReader(this.format, this.fileIn, conf, split.getLength());
            this.filePosition = this.fileIn;
        }
        
        this.pos = this.start;
    }
    
    private long getFilePosition() throws IOException {
        long retVal;
        if (this.isCompressedInput && null != this.filePosition) {
            retVal = this.filePosition.getPos();
        } else {
            retVal = this.pos;
        }
        return retVal;
    }
    
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (this.key == null) {
            this.key = new LongWritable();
        }
        this.key.set(this.pos);
        
        if (this.value == null) {
            this.value = new ReadInfo(this.filename);
        }
        if (this.rawValue == null) {
            this.rawValue = new Read();
        }
        
        long newSize = 0;
        
        newSize = this.in.readRead(this.rawValue);
        this.pos += newSize;
        
        if(this.rawValue.isEmpty()) {
            this.key = null;
            this.rawValue = null;
            this.value = null;
            return false;
        } else {
            this.value.setDescription(this.rawValue.getDescription());
            this.value.setSequence(this.rawValue.getFullSequence());
            return true;
        }
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return this.key;
    }

    @Override
    public ReadInfo getCurrentValue() throws IOException, InterruptedException {
        return this.value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (this.start == this.end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (getFilePosition() - this.start) / (float) (this.end - this.start));
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (this.in != null) {
                this.in.close();
            }
        } finally {
            if (this.decompressor != null) {
                CodecPool.returnDecompressor(this.decompressor);
                this.decompressor = null;
            }
        }
    }

}
