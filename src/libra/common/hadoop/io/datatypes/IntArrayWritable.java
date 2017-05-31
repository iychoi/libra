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
public class IntArrayWritable extends BinaryComparable implements WritableComparable<BinaryComparable> {

    private static final Log LOG = LogFactory.getLog(IntArrayWritable.class);
    
    private int[] intArray;
    
    private byte[] prevBytes;
    
    public IntArrayWritable() {}
    
    public IntArrayWritable(IntArrayWritable writable) { set(writable); }
    
    public IntArrayWritable(int[] intArray) { set(intArray); }
    
    public IntArrayWritable(List<Integer> intArray) { set(intArray); }
    
    /**
     * Set the value.
     */
    public void set(int[] intArray) {
        this.intArray = intArray;
        this.prevBytes = null;
    }
    
    public void set(List<Integer> intArray) {
        int[] arr = new int[intArray.size()];
        for(int i=0;i<intArray.size();i++) {
            arr[i] = intArray.get(i);    
        }
        this.intArray = arr;
        this.prevBytes = null;
    }
    
    public void set(IntArrayWritable that) {
        this.intArray = that.intArray;
        this.prevBytes = that.prevBytes;
    }
    
    public void setEmpty() {
        this.intArray = null;
        this.prevBytes = null;
    }
    
    public boolean isEmpty() {
        if(this.intArray == null) {
            return true;
        }
        return false;
    }

    /**
     * Return the value.
     */
    public int[] get() {
        return this.intArray;
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        int count = in.readInt();
        
        int[] arr = new int[count];
        for (int i = 0; i < count; i++) {
            arr[i] = in.readInt();
        }
        
        this.intArray = arr;
        this.prevBytes = null;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        int count = this.intArray.length;
        out.writeInt(count);
        
        for (int i = 0; i < count; i++) {
            out.writeInt(this.intArray[i]);
        }
    }
    
    /**
     * Returns true iff
     */
    @Override
    public boolean equals(Object o) {
        if (o instanceof IntArrayWritable) {
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
        
        for(int i=0;i<this.intArray.length;i++) {
            if(value.length() != 0) {
                value += ",";
            }
            value += this.intArray[i];
        }
        return value;
    }
    
    @Override
    public int getLength() {
        return this.intArray.length * 4;
    }

    @Override
    public byte[] getBytes() {
        if(this.prevBytes == null) {
            byte[] arr = new byte[this.intArray.length * 4];
            for(int i=0;i<this.intArray.length;i++) {
                int ivalue = this.intArray[i];
                arr[4*i] = (byte) ((ivalue >> 24) & 0xff);
                arr[4*i+1] = (byte) ((ivalue >> 16) & 0xff);
                arr[4*i+2] = (byte) ((ivalue >> 8) & 0xff);
                arr[4*i+3] = (byte) (ivalue & 0xff);
            }
            this.prevBytes = arr;
        }
        return prevBytes;
    }
    
    /** A Comparator optimized for IntArrayWritable. */ 
    public static class Comparator extends WritableComparator {

        public Comparator() {
            super(IntArrayWritable.class);
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
        WritableComparator.define(IntArrayWritable.class, new IntArrayWritable.Comparator());
    }
}
