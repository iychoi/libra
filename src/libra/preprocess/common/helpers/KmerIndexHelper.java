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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import libra.common.helpers.FileSystemHelper;
import libra.preprocess.common.PreprocessorConstants;
import libra.preprocess.common.kmerindex.KmerIndexTablePathFilter;
import libra.preprocess.common.kmerindex.KmerIndexDataPathFilter;
import libra.preprocess.stage2.KmerIndexBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;

/**
 *
 * @author iychoi
 */
public class KmerIndexHelper {
    
    private static final Log LOG = LogFactory.getLog(KmerIndexHelper.class);
    
    private final static String KMER_INDEX_TABLE_PATH_EXP = ".+\\." + PreprocessorConstants.KMER_INDEX_TABLE_FILENAME_EXTENSION + "$";
    private final static Pattern KMER_INDEX_TABLE_PATH_PATTERN = Pattern.compile(KMER_INDEX_TABLE_PATH_EXP);
    private final static String KMER_INDEX_DATA_PATH_EXP = ".+\\." + PreprocessorConstants.KMER_INDEX_DATA_FILENAME_EXTENSION + "\\.\\d+$";
    private final static Pattern KMER_INDEX_DATA_PATH_PATTERN = Pattern.compile(KMER_INDEX_DATA_PATH_EXP);
    
    public static String makeKmerIndexTableFileName(String filename) {
        return filename + "." + PreprocessorConstants.KMER_INDEX_TABLE_FILENAME_EXTENSION;
    }
    
    public static String makeKmerIndexDataFileName(Path filePath, int mapreduceID) {
        return makeKmerIndexDataFileName(filePath.getName(), mapreduceID);
    }
    
    public static String makeKmerIndexDataFileName(String filename, int mapreduceID) {
        return filename + "." + PreprocessorConstants.KMER_INDEX_DATA_FILENAME_EXTENSION + "." + mapreduceID;
    }
    
    public static boolean isKmerIndexTableFile(Path path) {
        return isKmerIndexTableFile(path.getName());
    }
    
    public static boolean isKmerIndexTableFile(String path) {
        Matcher matcher = KMER_INDEX_TABLE_PATH_PATTERN.matcher(path.toLowerCase());
        if(matcher.matches()) {
            return true;
        }
        return false;
    }
    
    public static boolean isKmerIndexDataFile(Path path) {
        return isKmerIndexDataFile(path.getName());
    }
    
    public static boolean isKmerIndexDataFile(String path) {
        Matcher matcher = KMER_INDEX_DATA_PATH_PATTERN.matcher(path.toLowerCase());
        if(matcher.matches()) {
            return true;
        }
        return false;
    }
    
    public static String getFileTableName(Path indexFilePath) {
        return getFileTableName(indexFilePath.getName());
    }
    
    public static String getFileTableName(String indexFileName) {
        if(isKmerIndexTableFile(indexFileName)) {
            int idx = indexFileName.lastIndexOf("." + PreprocessorConstants.KMER_INDEX_TABLE_FILENAME_EXTENSION);
            if (idx >= 0) {
                String tableFilePath = indexFileName.substring(0, idx);
                int idx2 = tableFilePath.lastIndexOf("/");
                if (idx2 >= 0) {
                    return tableFilePath.substring(idx2 + 1);
                } else {
                    return tableFilePath;
                }
            }
        } else if(isKmerIndexDataFile(indexFileName)) {
            int idx = indexFileName.lastIndexOf("." + PreprocessorConstants.KMER_INDEX_DATA_FILENAME_EXTENSION);
            if (idx >= 0) {
                String tableFilePath = indexFileName.substring(0, idx);
                int idx2 = tableFilePath.lastIndexOf("/");
                if (idx2 >= 0) {
                    return tableFilePath.substring(idx2 + 1);
                } else {
                    return tableFilePath;
                }
            }
        }
        LOG.error("Not an index table or index data file - " + indexFileName);
        return null;
    }
    
    public static boolean isSameKmerIndex(Path index1, Path index2) {
        return isSameKmerIndex(index1.getName(), index2.getName());
    }
    
    public static boolean isSameKmerIndex(String index1, String index2) {
        String fileTableName1 = getFileTableName(index1);
        String fileTableName2 = getFileTableName(index2);
        
        if(fileTableName1 == null || fileTableName2 == null) {
            return false;
        }
        
        return fileTableName1.equals(fileTableName2);
    }
    
    public static int getIndexDataID(Path indexFilePath) {
        return getIndexDataID(indexFilePath.getName());
    }
    
    public static int getIndexDataID(String indexFileName) {
        int idx = indexFileName.lastIndexOf(".");
        if(idx >= 0) {
            String partID = indexFileName.substring(idx + 1);
            return Integer.parseInt(partID);
        }
        return -1;
    }
    
    public static Path[] getKmerIndexDataFilePath(Configuration conf, Path inputPath) throws IOException {
        List<Path> inputFiles = new ArrayList<Path>();
        KmerIndexDataPathFilter filter = new KmerIndexDataPathFilter();
        
        FileSystem fs = inputPath.getFileSystem(conf);
        if(fs.exists(inputPath)) {
            FileStatus status = fs.getFileStatus(inputPath);
            if(status.isDirectory()) {
                if(filter.accept(inputPath)) {
                    inputFiles.add(inputPath);
                }
                
                // check child
                FileStatus[] entries = fs.listStatus(inputPath);
                for (FileStatus entry : entries) {
                    if(entry.isDirectory()) {
                        if (filter.accept(entry.getPath())) {
                            inputFiles.add(entry.getPath());
                        }
                    }
                }
            }
        }
        
        return inputFiles.toArray(new Path[0]);
    }
    
    public static Path[][] groupKmerIndices(Path[] inputIndexPaths) {
        List<Path[]> groups = new ArrayList<Path[]>();
        
        List<Path> sortedInputIndexPaths = sortPath(inputIndexPaths);
        List<Path> group = new ArrayList<Path>();
        for(Path path: sortedInputIndexPaths) {
            if(group.isEmpty()) {
                group.add(path);
            } else {
                Path prev = group.get(0);
                if(isSameKmerIndex(prev, path)) {
                    group.add(path);
                } else {
                    groups.add(group.toArray(new Path[0]));
                    group.clear();
                    group.add(path);
                }
            }
        }
        
        if(!group.isEmpty()) {
            groups.add(group.toArray(new Path[0]));
            group.clear();
        }
        
        return groups.toArray(new Path[0][0]);
    }
    
    private static List<Path> sortPath(Path[] paths) {
        List<Path> pathList = new ArrayList<Path>();
        pathList.addAll(Arrays.asList(paths));
        
        Collections.sort(pathList, new Comparator<Path>() {

            @Override
            public int compare(Path t, Path t1) {
                String ts = t.getName();
                String t1s = t1.getName();

                return ts.compareTo(t1s);
            }
        });
        return pathList;
    }
}
