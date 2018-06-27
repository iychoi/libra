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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;

/**
 *
 * @author iychoi
 */
public class CompressedSplitReadReader extends SplitReadReader {
    
    private static final Log LOG = LogFactory.getLog(CompressedSplitReadReader.class);
    
    private SplitCompressionInputStream scin;
    protected boolean finished = false;
    
    public CompressedSplitReadReader(SampleFormat format, SplitCompressionInputStream in, Configuration conf) throws IOException {
        super(format, in, conf);
        
        this.scin = in;
        this.finished = false;
    }
    
    @Override
    public long readRead(Read read) throws IOException {
        long bytesConsumed = 0;
        long readSize = 0;
        read.clear();
        
        if(!this.finished) {
            if(this.scin.getPos() > this.scin.getAdjustedEnd()) {
                this.finished = true;
                return bytesConsumed;
            }
            
            readSize = super.skipIncompleteRead();
            bytesConsumed += readSize;
            if(this.scin.getPos() > this.scin.getAdjustedEnd()) {
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
            if(this.scin.getPos() > this.scin.getAdjustedEnd()) {
                this.finished = true;
                return bytesConsumed;
            }
        }
        return bytesConsumed;
    }
}
