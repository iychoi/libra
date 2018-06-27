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
import java.io.InputStream;
import libra.common.sequence.Read;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author iychoi
 */
public class UncompressedSplitReadReader extends SplitReadReader {
    
    private static final Log LOG = LogFactory.getLog(UncompressedSplitReadReader.class);
    
    protected long splitLength = 0;
    protected long totalBytesRead = 0;
    protected boolean finished = false;
    
    public UncompressedSplitReadReader(SampleFormat format, InputStream in, Configuration conf, long splitLength) throws IOException {
        super(format, in, conf);
        
        this.splitLength = splitLength;
        this.totalBytesRead = 0;
        this.finished = false;
    }
    
    @Override
    public long readRead(Read read) throws IOException {
        long bytesConsumed = 0;
        long readSize = 0;
        read.clear();
        
        if(!this.finished) {
            readSize = super.skipIncompleteRead();
            bytesConsumed += readSize;
            this.totalBytesRead += readSize;
            if(this.totalBytesRead >= this.splitLength) {
                // actual read starts after the split
                // no content in this block
                this.finished = true;
                return bytesConsumed;
            }

            readSize = super.readRead(read);
            if(readSize <= 0) {
                //EOF
                this.finished = true;
                return bytesConsumed;
            }
            
            bytesConsumed += readSize;
            this.totalBytesRead += readSize;
            if(this.totalBytesRead >= this.splitLength) {
                this.finished = true;
            }
        }
        return bytesConsumed;
    }
}
