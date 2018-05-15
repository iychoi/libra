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
import java.util.Collections;
import java.util.Comparator;
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
    
    private List<SampleInfo> getSampleInfoArray(Path[] samples, Configuration conf) throws IOException {
        List<SampleInfo> sampleInfoArray = new ArrayList<SampleInfo>();
        
        for(Path path : samples) {
            FileSystem fs = path.getFileSystem(conf);
            FileStatus status = fs.getFileStatus(path);
            if(status != null && status.isFile()) {
                SampleInfo sample = new SampleInfo(status);
                sampleInfoArray.add(sample);
            }
        }
        
        Collections.sort(sampleInfoArray, new SampleInfoComparator());
        return sampleInfoArray;
    }
    
    private List<SampleGroup> groupBySize(List<SampleInfo> sampleInfoArray, long maxSize) {
        List<SampleGroup> groups = new ArrayList<SampleGroup>();
        
        long totalSize = 0;
        SampleGroup group = new SampleGroup();

        for(SampleInfo sampleInfo : sampleInfoArray) {
            group.addSample(sampleInfo);
            totalSize += sampleInfo.getSize();
            
            if(totalSize >= this.groupSize * 0.9) {
                // if over 90% is filled
                totalSize = 0;
                groups.add(group);
                group = new SampleGroup();
            }
        }
        
        if(group.numSamples() > 0) {
            groups.add(group);
        }
        
        return groups;
    }
    
    private List<SampleGroup> mergeGroups(List<SampleGroup> groups, int maxGroupNum) {
        SampleGroup[] mergedGroups = new SampleGroup[maxGroupNum];
        List<SampleGroup> remainings = new ArrayList<SampleGroup>();
        
        int groupIndex = 0;
        for(SampleGroup group : groups) {
            if(groupIndex < maxGroupNum) {
                mergedGroups[groupIndex] = new SampleGroup();
                mergedGroups[groupIndex].addSamples(group.getSamples());
            } else {
                remainings.add(group);
            }
            
            groupIndex++;
        }
        
        // remaining...
        List<SampleInfo> remainingSamples = new ArrayList<SampleInfo>();
        for(SampleGroup group : remainings) {
            remainingSamples.addAll(group.getSamples());
        }
        Collections.sort(remainingSamples, new SampleInfoComparator());

        // put the largest sample remaining to the smallest group
        for(SampleInfo sampleInfo : remainingSamples) {
            // find the smallest group
            int groupSmallestIndex = -1;
            long groupSmallestSize = -1;
            for(int i=0;i<maxGroupNum;i++) {
                if(groupSmallestIndex < 0) {
                    groupSmallestIndex = i;
                    groupSmallestSize = mergedGroups[i].totalSampleSize();
                } else {
                    if(groupSmallestSize > mergedGroups[i].totalSampleSize()) {
                        // update smallest
                        groupSmallestIndex = i;
                        groupSmallestSize = mergedGroups[i].totalSampleSize();
                    }
                }
            }

            // put
            if(groupSmallestIndex >= 0) {
                mergedGroups[groupSmallestIndex].addSample(sampleInfo);
            }
        }
        
        List<SampleGroup> newGroups = new ArrayList<SampleGroup>();
        for(SampleGroup group: mergedGroups) {
            newGroups.add(group);
        }
        return newGroups;
    }
    
    public SampleGroup[] group(Path[] samples, Configuration conf) throws IOException {
        List<SampleInfo> sampleInfoArray = getSampleInfoArray(samples, conf);
        
        // group by size
        List<SampleGroup> groups = groupBySize(sampleInfoArray, this.groupSize);
        
        // if number of groups exceeds max group num
        if(groups.size() > this.maxGroupNum) {
            // merge
            groups = mergeGroups(groups, this.maxGroupNum);
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
