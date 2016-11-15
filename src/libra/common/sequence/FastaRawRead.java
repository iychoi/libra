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
package libra.common.sequence;

/**
 *
 * @author iychoi
 */
public class FastaRawRead {
    private String filename;
    private long read_offset;
    private long description_offset;
    private long sequence_offset;
    private long read_len;
    private long description_len;
    private long sequence_len;
    private String description;
    private boolean continuous_read = false;
    
    private FastaRawReadLine[] raw_sequence;
    
    public FastaRawRead(String filename) {
        this.filename = filename;
    }

    public String getFileName() {
        return this.filename;
    }
    
    public void setReadOffset(long offset) {
        this.read_offset = offset;
    }
    
    public long getReadOffset() {
        return this.read_offset;
    }
    
    public void setDescriptionOffset(long offset) {
        this.description_offset = offset;
    }
    
    public long getDescriptionOffset() {
        return this.description_offset;
    }
    
    public void setSequenceOffset(long offset) {
        this.sequence_offset = offset;
    }
    
    public long getSequenceOffset() {
        return this.sequence_offset;
    }
    
    public void setReadLen(long len) {
        this.read_len = len;
    }
    
    public long getReadLen() {
        return this.read_len;
    }
    
    public void setDescriptionLen(long len) {
        this.description_len = len;
    }
    
    public long getDescriptionLen() {
        return this.description_len;
    }
    
    public void setSequenceLen(long len) {
        this.sequence_len = len;
    }
    
    public long getSequenceLen() {
        return this.sequence_len;
    }
    
    public void setDescription(String description) {
        this.description = description;
    }
    
    public String getDescription() {
        return this.description;
    }
    
    public void setRawSequence(FastaRawReadLine[] raw_sequence) {
        this.raw_sequence = raw_sequence;
    }
    
    public FastaRawReadLine[] getRawSequence() {
        return this.raw_sequence;
    }
    
    public void setContinuousRead(boolean continuous_read) {
        this.continuous_read = continuous_read;
    }
    
    public boolean getContinuousRead() {
        return this.continuous_read;
    }
}
