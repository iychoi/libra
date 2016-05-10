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
package libra.preprocess.common.helpers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import libra.common.helpers.FileSystemHelper;
import libra.preprocess.common.PreprocessorConstants;
import libra.preprocess.common.kmerhistogram.KmerHistogramPathFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author iychoi
 */
public class KmerHistogramHelper {
    private final static String KMER_HISTOGRAM_PATH_EXP = ".+\\." + PreprocessorConstants.KMER_HISTOGRAM_FILENAME_EXTENSION + "$";
    private final static Pattern KMER_HISTOGRAM_PATH_PATTERN = Pattern.compile(KMER_HISTOGRAM_PATH_EXP);
    
    public static String makeKmerHistogramFileName(String sampleFileName) {
        return sampleFileName + "." + PreprocessorConstants.KMER_HISTOGRAM_FILENAME_EXTENSION;
    }
    
    public static String getSampleFileName(String histogramFileName) {
        int idx = histogramFileName.lastIndexOf("." + PreprocessorConstants.KMER_HISTOGRAM_FILENAME_EXTENSION);
        if(idx > 0) {
            return histogramFileName.substring(0, idx);
        }
        return histogramFileName;
    }
    
    public static boolean isKmerHistogramFile(Path path) {
        return isKmerHistogramFile(path.getName());
    }
    
    public static boolean isKmerHistogramFile(String path) {
        Matcher matcher = KMER_HISTOGRAM_PATH_PATTERN.matcher(path.toLowerCase());
        if(matcher.matches()) {
            return true;
        }
        return false;
    }
    
    public static Path[] getKmerHistogramFilePath(Configuration conf, String[] inputPaths) throws IOException {
        return getKmerHistogramFilePath(conf, FileSystemHelper.makePathFromString(conf, inputPaths));
    }
    
    public static Path[] getKmerHistogramFilePath(Configuration conf, Collection<String> inputPaths) throws IOException {
        return getKmerHistogramFilePath(conf, FileSystemHelper.makePathFromString(conf, inputPaths));
    }
    
    public static Path[] getKmerHistogramFilePath(Configuration conf, Path[] inputPaths) throws IOException {
        List<Path> inputFiles = new ArrayList<Path>();
        KmerHistogramPathFilter filter = new KmerHistogramPathFilter();
        
        for(Path path : inputPaths) {
            FileSystem fs = path.getFileSystem(conf);
            if(fs.exists(path)) {
                FileStatus status = fs.getFileStatus(path);
                if(status.isDir()) {
                    if(filter.accept(path)) {
                        inputFiles.add(path);
                    } else {
                        // check child
                        FileStatus[] entries = fs.listStatus(path);
                        for (FileStatus entry : entries) {
                            if(entry.isDir()) {
                                if (filter.accept(entry.getPath())) {
                                    inputFiles.add(entry.getPath());
                                }
                            }
                        }
                    }
                }
            }
        }
        
        Path[] files = inputFiles.toArray(new Path[0]);
        return files;
    }
}
