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
package libra.common.hadoop.io.datatypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import libra.common.helpers.SequenceHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 *
 * @author iychoi
 */
public class CompressedSequenceWritable extends BinaryComparable implements WritableComparable<BinaryComparable> {

    private static final Log LOG = LogFactory.getLog(CompressedSequenceWritable.class);
    
    private byte[] compressedSequence;
    private int seqLength;
    
    private static final int LENGTH_BYTES = 1;
    
    public CompressedSequenceWritable() {}
    
    public CompressedSequenceWritable(String sequence) throws IOException { set(sequence); }
    
    public CompressedSequenceWritable(byte[] compressedSequence, int seqLength) { set(compressedSequence, seqLength); }
    
    /**
     * Set the value.
     */
    public void set(byte[] compressedSequence, int seqLength) {
        this.compressedSequence = compressedSequence;
        this.seqLength = seqLength;
    }
    
    public void set(String sequence) throws IOException {
        this.compressedSequence = SequenceHelper.compress(sequence);
        this.seqLength = sequence.length();
    }
    
    public void set(CompressedSequenceWritable that) throws IOException {
        this.compressedSequence = that.compressedSequence;
        this.seqLength = that.seqLength;
    }
    
    public void setEmpty() {
        this.compressedSequence = null;
        this.seqLength = 0;
    }

    public boolean isEmpty() {
        if(this.compressedSequence == null || this.seqLength == 0) {
            return true;
        }
        return false;
    }
    
    /**
     * Return the value.
     */
    public byte[] getCompressedSequence() {
        return this.compressedSequence;
    }
    
    public String getSequence() {
        return SequenceHelper.decompress(this.compressedSequence, this.seqLength);
    }
    
    public int getSequenceLength() {
        return this.seqLength;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.seqLength = in.readByte();
        int byteLen = SequenceHelper.getCompressedSize(this.seqLength);
        this.compressedSequence = new byte[byteLen];
        in.readFully(this.compressedSequence, 0, byteLen);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeByte(this.seqLength);
        out.write(this.compressedSequence);
    }
    
    /**
     * Returns true iff
     */
    @Override
    public boolean equals(Object o) {
        if (o instanceof CompressedSequenceWritable) {
            return super.equals(o);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public String toString() {
        return SequenceHelper.decompress(this.compressedSequence, this.seqLength);
    }

    @Override
    public int getLength() {
        return this.compressedSequence.length;
    }

    @Override
    public byte[] getBytes() {
        return this.compressedSequence;
    }
    
    /** A Comparator optimized for CompressedSequenceWritable. */ 
    public static class Comparator extends WritableComparator {

        public Comparator() {
            super(CompressedSequenceWritable.class);
        }

        /**
         * Compare the buffers in serialized form.
         */
        public int compare(byte[] b1, int s1, int l1,
                byte[] b2, int s2, int l2) {
            return compareBytes(b1, s1 + LENGTH_BYTES, l1 - LENGTH_BYTES,
                    b2, s2 + LENGTH_BYTES, l2 - LENGTH_BYTES);
        }
    }

    static {
        // register this comparator
        WritableComparator.define(CompressedSequenceWritable.class, new Comparator());
    }
}
