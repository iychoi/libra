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
package libra.core.common.helpers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import libra.common.helpers.FileSystemHelper;
import libra.core.commom.CoreConstants;
import libra.core.common.kmersimilarity.KmerSimilarityResultPathFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author iychoi
 */
public class KmerSimilarityHelper {
    private final static String KMER_SIMILARITY_RESULT_PATH_EXP = ".+\\." + CoreConstants.KMER_SIMILARITY_RESULT_FILENAME_EXTENSION + "\\.\\d+$";
    private final static Pattern KMER_SIMILARITY_RESULT_PATH_PATTERN = Pattern.compile(KMER_SIMILARITY_RESULT_PATH_EXP);
    
    public static boolean isKmerSimilarityTableFile(Path path) {
        return isKmerSimilarityTableFile(path.getName());
    }
    
    public static boolean isKmerSimilarityTableFile(String path) {
        if(path.compareToIgnoreCase(CoreConstants.KMER_SIMILARITY_TABLE_FILENAME) == 0) {
            return true;
        }
        return false;
    }
    
    public static boolean isKmerSimilarityResultFile(Path path) {
        return isKmerSimilarityResultFile(path.getName());
    }
    
    public static boolean isKmerSimilarityResultFile(String path) {
        Matcher matcher = KMER_SIMILARITY_RESULT_PATH_PATTERN.matcher(path.toLowerCase());
        if(matcher.matches()) {
            return true;
        }
        return false;
    }
    
    public static String makeKmerSimilarityTableFileName() {
        return CoreConstants.KMER_SIMILARITY_TABLE_FILENAME;
    }
    
    public static String makeKmerSimilarityResultFileName(int mapreduceID) {
        return CoreConstants.KMER_SIMILARITY_RESULT_FILENAME_PREFIX + "." + CoreConstants.KMER_SIMILARITY_RESULT_FILENAME_EXTENSION + "." + mapreduceID;
    }
    
    public static String makeKmerSimilarityFinalResultFileName() {
        return CoreConstants.KMER_SIMILARITY_RESULT_FILENAME_PREFIX + "." + CoreConstants.KMER_SIMILARITY_RESULT_FILENAME_EXTENSION;
    }
    
    public static Path[] getAllKmerSimilarityResultFilePath(Configuration conf, String inputPathsCommaSeparated) throws IOException {
        return getAllKmerSimilarityResultFilePath(conf, FileSystemHelper.makePathFromString(conf, FileSystemHelper.splitCommaSeparated(inputPathsCommaSeparated)));
    }
    
    public static Path[] getAllKmerSimilarityResultFilePath(Configuration conf, String[] inputPath) throws IOException {
        return getAllKmerSimilarityResultFilePath(conf, FileSystemHelper.makePathFromString(conf, inputPath));
    }
    
    public static Path[] getAllKmerSimilarityResultFilePath(Configuration conf, Path[] inputPaths) throws IOException {
        List<Path> inputFiles = new ArrayList<Path>();
        KmerSimilarityResultPathFilter filter = new KmerSimilarityResultPathFilter();
        
        for(Path path : inputPaths) {
            FileSystem fs = path.getFileSystem(conf);
            if(fs.exists(path)) {
                FileStatus status = fs.getFileStatus(path);
                if(status.isDir()) {
                    // check child
                    FileStatus[] entries = fs.listStatus(path);
                    for (FileStatus entry : entries) {
                        if(!entry.isDir()) {
                            if (filter.accept(entry.getPath())) {
                                inputFiles.add(entry.getPath());
                            }
                        }
                    }
                } else {
                    if (filter.accept(status.getPath())) {
                        inputFiles.add(status.getPath());
                    }
                }
            }
        }
        
        Path[] files = inputFiles.toArray(new Path[0]);
        return files;
    }
}
