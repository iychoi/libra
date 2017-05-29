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
package libra.preprocess.common.filetable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import libra.common.helpers.CryptoHelper;
import libra.common.helpers.FileSystemHelper;
import libra.common.helpers.NameHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author iychoi
 */
public class SampleGrouper {
    
    private static final Log LOG = LogFactory.getLog(SampleGrouper.class);
    
    private long groupSize;
    private int maxGroupNum;
    
    public SampleGrouper(long groupSize, int maxGroupNum) {
        this.groupSize = groupSize;
        this.maxGroupNum = maxGroupNum;
    }
    
    public FileTable[] group(Path[] samples, int kmerSize, Configuration conf) throws IOException {
        List<FileTable> tables = new ArrayList<FileTable>();
        
        long size = 0;
        FileTable tbl = new FileTable();
        
        for(Path path : samples) {
            FileSystem fs = path.getFileSystem(conf);
            FileStatus status = fs.getFileStatus(path);
            if(status != null && status.isFile()) {
                size += status.getLen();
                tbl.addSample(path.toString());
                
                if(size >= this.groupSize) {
                    size = 0;
                    tables.add(tbl);
                    tbl = new FileTable();
                }
            }
        }
        
        if(tbl.samples() > 0) {
            tables.add(tbl);
        }
        
        if(tables.size() > this.maxGroupNum) {
            // fold
            int factor = tables.size() / this.maxGroupNum;
            if(tables.size() % this.maxGroupNum != 0) {
                factor++;
            }
            
            List<FileTable> new_tables = new ArrayList<FileTable>();
            FileTable new_tbl = new FileTable();
            int count = 0;
            
            for(FileTable g : tables) {
                new_tbl.addSample(g.getSamples());
                count++;
                
                if(count >= factor) {
                    count = 0;
                    new_tables.add(new_tbl);
                    new_tbl = new FileTable();
                }
            }
            
            if(new_tbl.samples() > 0) {
                new_tables.add(new_tbl);
            }
            
            tables = new_tables;
        }
        
        for(FileTable t : tables) {
            t.setName(makeNameByContent(t.getSamples()));
            t.setKmerSize(kmerSize);
        }
        
        return tables.toArray(new FileTable[0]);
    }
    
    public FileTable[] group(Collection<String> samples, int kmerSize, Configuration conf) throws IOException {
        Path[] paths = FileSystemHelper.makePathFromString(conf, samples);
        
        return group(paths, kmerSize, conf);
    }

    private String makeNameByContent(Collection<String> samples) {
        try {
            String paths = FileSystemHelper.makeCommaSeparated(samples);
            String sha1sum = CryptoHelper.sha1sum(paths);
            return sha1sum;
        } catch (IOException ex) {
            LOG.error(ex);
            LOG.info("Generating random name");
            return NameHelper.generateRandomString(32);
        }
    }
}
