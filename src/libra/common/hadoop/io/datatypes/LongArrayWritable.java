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
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import static org.apache.hadoop.io.WritableComparator.compareBytes;

/**
 *
 * @author iychoi
 */
public class LongArrayWritable extends BinaryComparable implements WritableComparable<BinaryComparable> {

    private static final Log LOG = LogFactory.getLog(LongArrayWritable.class);
    
    private long[] longArray;
    
    private byte[] prevBytes;
    
    public LongArrayWritable() {}
    
    public LongArrayWritable(LongArrayWritable writable) { set(writable); }
    
    public LongArrayWritable(long[] longArray) { set(longArray); }
    
    public LongArrayWritable(List<Long> longArray) { set(longArray); }
    
    /**
     * Set the value.
     */
    public void set(long[] longArray) {
        this.longArray = longArray;
        this.prevBytes = null;
    }
    
    public void set(List<Long> longArray) {
        long[] arr = new long[longArray.size()];
        for(int i=0;i<longArray.size();i++) {
            arr[i] = longArray.get(i);    
        }
        this.longArray = arr;
        this.prevBytes = null;
    }
    
    public void set(LongArrayWritable that) {
        this.longArray = that.longArray;
        this.prevBytes = that.prevBytes;
    }
    
    public void setEmpty() {
        this.longArray = null;
        this.prevBytes = null;
    }
    
    public boolean isEmpty() {
        if(this.longArray == null) {
            return true;
        }
        return false;
    }

    /**
     * Return the value.
     */
    public long[] get() {
        return this.longArray;
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        int count = in.readInt();
        
        long[] arr = new long[count];
        for (int i = 0; i < count; i++) {
            arr[i] = in.readLong();
        }
        
        this.longArray = arr;
        this.prevBytes = null;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        int count = this.longArray.length;
        out.writeInt(count);
        
        for (int i = 0; i < count; i++) {
            out.writeLong(this.longArray[i]);
        }
    }
    
    /**
     * Returns true iff
     */
    @Override
    public boolean equals(Object o) {
        if (o instanceof LongArrayWritable) {
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
        String value = new String();
        
        for(int i=0;i<this.longArray.length;i++) {
            if(value.length() != 0) {
                value += ",";
            }
            value += this.longArray[i];
        }
        return value;
    }
    
    @Override
    public int getLength() {
        return this.longArray.length * 8;
    }

    @Override
    public byte[] getBytes() {
        if(this.prevBytes == null) {
            byte[] arr = new byte[this.longArray.length * 8];
            for(int i=0;i<this.longArray.length;i++) {
                long lvalue = this.longArray[i];
                arr[8*i] = (byte) ((lvalue >> 56) & 0xff);
                arr[8*i+1] = (byte) ((lvalue >> 48) & 0xff);
                arr[8*i+2] = (byte) ((lvalue >> 40) & 0xff);
                arr[8*i+3] = (byte) ((lvalue >> 32) & 0xff);
                arr[8*i+4] = (byte) ((lvalue >> 24) & 0xff);
                arr[8*i+5] = (byte) ((lvalue >> 16) & 0xff);
                arr[8*i+6] = (byte) ((lvalue >> 8) & 0xff);
                arr[8*i+7] = (byte) (lvalue & 0xff);
            }
            this.prevBytes = arr;
        }
        return prevBytes;
    }
    
    /** A Comparator optimized for IntArrayWritable. */ 
    public static class Comparator extends WritableComparator {

        public Comparator() {
            super(LongArrayWritable.class);
        }

        /**
         * Compare the buffers in serialized form.
         */
        @Override
        public int compare(byte[] b1, int s1, int l1,
                byte[] b2, int s2, int l2) {
            return compareBytes(b1, s1, l1,
                    b2, s2, l2);
        }
    }

    static {
        // register this comparator
        WritableComparator.define(LongArrayWritable.class, new LongArrayWritable.Comparator());
    }
}
