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
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import libra.preprocess.common.PreprocessorConstants;
import libra.preprocess.common.kmerfilter.KmerFilterPartTablePathFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author iychoi
 */
public class KmerFilterHelper {
    
    private final static String KMER_FILTER_TABLE_PATH_EXP = ".+\\." + PreprocessorConstants.KMER_FILTER_TABLE_FILENAME_EXTENSION + "$";
    private final static Pattern KMER_FILTER_TABLE_PATH_PATTERN = Pattern.compile(KMER_FILTER_TABLE_PATH_EXP);
    
    private final static String KMER_FILTER_PART_TABLE_PATH_EXP = ".+\\." + PreprocessorConstants.KMER_FILTER_TABLE_FILENAME_EXTENSION + "\\.\\d+$";
    private final static Pattern KMER_FILTER_PART_TABLE_PATH_PATTERN = Pattern.compile(KMER_FILTER_PART_TABLE_PATH_EXP);
    
    public static String makeKmerFilterTableFileName(String filename) {
        return filename + "." + PreprocessorConstants.KMER_FILTER_TABLE_FILENAME_EXTENSION;
    }
    
    public static String makeKmerFilterPartTableFileName(String filename, int taskID) {
        return filename + "." + PreprocessorConstants.KMER_FILTER_TABLE_FILENAME_EXTENSION + "." + taskID;
    }
    
    public static boolean isKmerFilterTableFile(Path path) {
        return isKmerFilterTableFile(path.getName());
    }
    
    public static boolean isKmerFilterTableFile(String path) {
        Matcher matcher = KMER_FILTER_TABLE_PATH_PATTERN.matcher(path.toLowerCase());
        if(matcher.matches()) {
            return true;
        }
        return false;
    }
    
    public static boolean isKmerFilterPartTableFile(Path path) {
        return isKmerFilterPartTableFile(path.getName());
    }
    
    public static boolean isKmerFilterPartTableFile(String path) {
        Matcher matcher = KMER_FILTER_PART_TABLE_PATH_PATTERN.matcher(path.toLowerCase());
        if(matcher.matches()) {
            return true;
        }
        return false;
    }
    
    public static Path[] getKmerFilterPartTableFilePath(Configuration conf, Path inputPath) throws IOException {
        List<Path> inputFiles = new ArrayList<Path>();
        KmerFilterPartTablePathFilter filter = new KmerFilterPartTablePathFilter();
        
        FileSystem fs = inputPath.getFileSystem(conf);
        if(fs.exists(inputPath)) {
            FileStatus status = fs.getFileStatus(inputPath);
            if(status.isDirectory()) {
                // check child
                FileStatus[] entries = fs.listStatus(inputPath);
                for (FileStatus entry : entries) {
                    if(entry.isFile()) {
                        if (filter.accept(entry.getPath())) {
                            inputFiles.add(entry.getPath());
                        }
                    }
                }
            } else {
                if (filter.accept(inputPath)) {
                    inputFiles.add(inputPath);
                }
            }
        }
        
        Path[] files = inputFiles.toArray(new Path[0]);
        return files;
    }
}
