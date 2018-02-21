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
package libra.distancematrix.common.helpers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import libra.distancematrix.common.DistanceMatrixConstants;
import libra.distancematrix.common.kmersimilarity.KmerSimilarityResultPartPathFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author iychoi
 */
public class KmerSimilarityHelper {
    private final static String KMER_SIMILARITY_RESULT_PART_PATH_EXP = ".+\\." + DistanceMatrixConstants.KMER_SIMILARITY_RESULT_FILENAME_EXTENSION + "\\.\\d+$";
    private final static Pattern KMER_SIMILARITY_RESULT_PART_PATH_PATTERN = Pattern.compile(KMER_SIMILARITY_RESULT_PART_PATH_EXP);
    
    public static boolean isKmerSimilarityFileMappingTableFile(Path path) {
        return isKmerSimilarityFileMappingTableFile(path.getName());
    }
    
    public static boolean isKmerSimilarityFileMappingTableFile(String path) {
        if(path.compareToIgnoreCase(DistanceMatrixConstants.KMER_SIMILARITY_FILE_MAPPING_TABLE_FILENAME) == 0) {
            return true;
        }
        return false;
    }
    
    public static boolean isKmerSimilarityResultPartFile(Path path) {
        return KmerSimilarityHelper.isKmerSimilarityResultPartFile(path.getName());
    }
    
    public static boolean isKmerSimilarityResultPartFile(String path) {
        Matcher matcher = KMER_SIMILARITY_RESULT_PART_PATH_PATTERN.matcher(path.toLowerCase());
        if(matcher.matches()) {
            return true;
        }
        return false;
    }
    
    public static String makeKmerSimilarityFileMappingTableFileName() {
        return DistanceMatrixConstants.KMER_SIMILARITY_FILE_MAPPING_TABLE_FILENAME;
    }
    
    public static String makeKmerSimilarityResultPartFileName(int mapreduceID) {
        return DistanceMatrixConstants.KMER_SIMILARITY_RESULT_FILENAME_PREFIX + "." + DistanceMatrixConstants.KMER_SIMILARITY_RESULT_FILENAME_EXTENSION + "." + mapreduceID;
    }
    
    public static String makeKmerSimilarityResultFileName() {
        return DistanceMatrixConstants.KMER_SIMILARITY_RESULT_FILENAME_PREFIX + "." + DistanceMatrixConstants.KMER_SIMILARITY_RESULT_FILENAME_EXTENSION;
    }
    
    public static Path[] getKmerSimilarityResultPartFilePath(Configuration conf, Path inputPath) throws IOException {
        List<Path> inputFiles = new ArrayList<Path>();
        KmerSimilarityResultPartPathFilter filter = new KmerSimilarityResultPartPathFilter();
        
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
                if (filter.accept(status.getPath())) {
                    inputFiles.add(status.getPath());
                }
            }
        }
        
        Path[] files = inputFiles.toArray(new Path[0]);
        return files;
    }
}
