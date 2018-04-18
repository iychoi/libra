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
import libra.common.helpers.PathHelper;
import libra.preprocess.common.PreprocessorConstants;
import libra.preprocess.common.kmerstatistics.KmerStatisticsPartTablePathFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author iychoi
 */
public class KmerStatisticsHelper {
    
    private final static String KMER_STATISTICS_TABLE_PATH_EXP = ".+\\." + PreprocessorConstants.KMER_STATISTICS_TABLE_FILENAME_EXTENSION + "$";
    private final static Pattern KMER_STATISTICS_TABLE_PATH_PATTERN = Pattern.compile(KMER_STATISTICS_TABLE_PATH_EXP);
    
    private final static String KMER_STATISTICS_PART_TABLE_PATH_EXP = ".+\\." + PreprocessorConstants.KMER_STATISTICS_TABLE_FILENAME_EXTENSION + "\\.\\d+$";
    private final static Pattern KMER_STATISTICS_PART_TABLE_PATH_PATTERN = Pattern.compile(KMER_STATISTICS_PART_TABLE_PATH_EXP);
    
    public static String makeKmerStatisticsTableFileName(String filename) {
        return filename + "." + PreprocessorConstants.KMER_STATISTICS_TABLE_FILENAME_EXTENSION;
    }
    
    public static String makeKmerStatisticsPartTableFileName(String filename, int taskID) {
        return filename + "." + PreprocessorConstants.KMER_STATISTICS_TABLE_FILENAME_EXTENSION + "." + taskID;
    }
    
    public static String makeKmerStatisticsDirPath(String rootPath) {
        return PathHelper.concatPath(rootPath, PreprocessorConstants.KMER_STATISITCS_DIRNAME);
    }
    
    public static boolean isKmerStatisticsTableFile(Path path) {
        return isKmerStatisticsTableFile(path.getName());
    }
    
    public static boolean isKmerStatisticsTableFile(String path) {
        Matcher matcher = KMER_STATISTICS_TABLE_PATH_PATTERN.matcher(path.toLowerCase());
        if(matcher.matches()) {
            return true;
        }
        return false;
    }
    
    public static boolean isKmerStatisticsPartTableFile(Path path) {
        return isKmerStatisticsPartTableFile(path.getName());
    }
    
    public static boolean isKmerStatisticsPartTableFile(String path) {
        Matcher matcher = KMER_STATISTICS_PART_TABLE_PATH_PATTERN.matcher(path.toLowerCase());
        if(matcher.matches()) {
            return true;
        }
        return false;
    }
    
    public static Path[] getKmerStatisticsPartTableFilePaths(Configuration conf, Path inputPath) throws IOException {
        List<Path> inputFiles = new ArrayList<Path>();
        KmerStatisticsPartTablePathFilter filter = new KmerStatisticsPartTablePathFilter();
        
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
