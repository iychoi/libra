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
package libra.preprocess.common.samplegroup;

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
    
    public SampleGroup[] group(Collection<String> samples, Configuration conf) throws IOException {
        Path[] paths = FileSystemHelper.makePathFromString(conf, samples);
        return group(paths, conf);
    }
    
    public SampleGroup[] group(Path[] samples, Configuration conf) throws IOException {
        List<SampleGroup> groups = new ArrayList<SampleGroup>();
        
        long size = 0;
        SampleGroup group = new SampleGroup();
        
        // use group size to group
        for(Path path : samples) {
            FileSystem fs = path.getFileSystem(conf);
            FileStatus status = fs.getFileStatus(path);
            if(status != null && status.isFile()) {
                size += status.getLen();
                SampleInfo sample = new SampleInfo(status);
                group.addSample(sample);
                
                if(size >= this.groupSize) {
                    size = 0;
                    groups.add(group);
                    group = new SampleGroup();
                }
            }
        }
        
        if(group.samples() > 0) {
            groups.add(group);
        }
        
        // if number of groups exceeds max group num
        if(groups.size() > this.maxGroupNum) {
            // fold
            int factor = groups.size() / this.maxGroupNum;
            if(groups.size() % this.maxGroupNum != 0) {
                factor++;
            }
            
            List<SampleGroup> new_groups = new ArrayList<SampleGroup>();
            SampleGroup new_tbl = new SampleGroup();
            int count = 0;
            
            for(SampleGroup g : groups) {
                new_tbl.addSample(g.getSamples());
                count++;
                
                if(count >= factor) {
                    count = 0;
                    new_groups.add(new_tbl);
                    new_tbl = new SampleGroup();
                }
            }
            
            if(new_tbl.samples() > 0) {
                new_groups.add(new_tbl);
            }
            
            groups = new_groups;
        }
        
        for(SampleGroup g : groups) {
            g.setName(makeNameByContent(g.getSamples()));
        }
        
        return groups.toArray(new SampleGroup[0]);
    }

    private String makeNameByContent(Collection<SampleInfo> samples) {
        List<String> samplenames = new ArrayList<String>();
        for(SampleInfo sample : samples) {
            samplenames.add(sample.getPath());
        }
        
        try {
            String paths = FileSystemHelper.makeCommaSeparated(samplenames);
            String sha1sum = CryptoHelper.sha1sum(paths);
            return sha1sum;
        } catch (IOException ex) {
            LOG.error(ex);
            LOG.info("Generating random name");
            return NameHelper.generateRandomString(32);
        }
    }
}
