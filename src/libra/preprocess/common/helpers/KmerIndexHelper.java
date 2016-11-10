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
import libra.preprocess.common.kmerindex.KmerIndexIndexPathFilter;
import libra.preprocess.common.kmerindex.KmerIndexPartPathFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;

/**
 *
 * @author iychoi
 */
@SuppressWarnings("deprecation")
public class KmerIndexHelper {
    private final static String KMER_INDEX_INDEX_PATH_EXP = ".+\\." + PreprocessorConstants.KMER_INDEX_INDEX_FILENAME_EXTENSION + "$";
    private final static Pattern KMER_INDEX_INDEX_PATH_PATTERN = Pattern.compile(KMER_INDEX_INDEX_PATH_EXP);
    private final static String KMER_INDEX_PART_PATH_EXP = ".+\\." + PreprocessorConstants.KMER_INDEX_PART_FILENAME_EXTENSION + "\\.\\d+$";
    private final static Pattern KMER_INDEX_PART_PATH_PATTERN = Pattern.compile(KMER_INDEX_PART_PATH_EXP);
    
    public static String makeKmerIndexIndexFileName(Path filePath, int kmerSize) {
        return makeKmerIndexIndexFileName(filePath.getName(), kmerSize);
    }
    
    public static String makeKmerIndexIndexFileName(String filename, int kmerSize) {
        return filename + "." + kmerSize + "." + PreprocessorConstants.KMER_INDEX_INDEX_FILENAME_EXTENSION;
    }
    
    public static String makeKmerIndexPartFileName(Path filePath, int kmerSize, int mapreduceID) {
        return makeKmerIndexPartFileName(filePath.getName(), kmerSize, mapreduceID);
    }
    
    public static String makeKmerIndexPartFileName(String filename, int kmerSize, int mapreduceID) {
        return filename + "." + kmerSize + "." + PreprocessorConstants.KMER_INDEX_PART_FILENAME_EXTENSION + "." + mapreduceID;
    }
    
    public static boolean isKmerIndexIndexFile(Path path) {
        return isKmerIndexIndexFile(path.getName());
    }
    
    public static boolean isKmerIndexIndexFile(String path) {
        Matcher matcher = KMER_INDEX_INDEX_PATH_PATTERN.matcher(path.toLowerCase());
        if(matcher.matches()) {
            return true;
        }
        return false;
    }
    
    public static boolean isKmerIndexPartFile(Path path) {
        return isKmerIndexPartFile(path.getName());
    }
    
    public static boolean isKmerIndexPartFile(String path) {
        Matcher matcher = KMER_INDEX_PART_PATH_PATTERN.matcher(path.toLowerCase());
        if(matcher.matches()) {
            return true;
        }
        return false;
    }
    
    public static String getFastaFileName(Path indexFilePath) {
        return getFastaFileName(indexFilePath.getName());
    }
    
    public static String getFastaFileName(String indexFileName) {
        if(isKmerIndexIndexFile(indexFileName)) {
            int idx = indexFileName.lastIndexOf("." + PreprocessorConstants.KMER_INDEX_INDEX_FILENAME_EXTENSION);
            if (idx >= 0) {
                String part = indexFileName.substring(0, idx);
                int idx2 = part.lastIndexOf(".");
                if (idx2 >= 0) {
                    String fastaFilePath = part.substring(0, idx2);
                    int idx3 = fastaFilePath.lastIndexOf("/");
                    if (idx3 >= 0) {
                        return fastaFilePath.substring(idx3 + 1);
                    } else {
                        return fastaFilePath;
                    }
                }
            }
        } else if(isKmerIndexPartFile(indexFileName)) {
            int idx = indexFileName.lastIndexOf("." + PreprocessorConstants.KMER_INDEX_PART_FILENAME_EXTENSION);
            if (idx >= 0) {
                String part = indexFileName.substring(0, idx);
                int idx2 = part.lastIndexOf(".");
                if (idx2 >= 0) {
                    String fastaFilePath = part.substring(0, idx2);
                    int idx3 = fastaFilePath.lastIndexOf("/");
                    if (idx3 >= 0) {
                        return fastaFilePath.substring(idx3 + 1);
                    } else {
                        return fastaFilePath;
                    }
                }
            }
        }
        return null;
    }
    
    public static boolean isSameKmerIndex(Path index1, Path index2) {
        return isSameKmerIndex(index1.getName(), index2.getName());
    }
    
    public static boolean isSameKmerIndex(String index1, String index2) {
        String fastaFileName1 = getFastaFileName(index1);
        String fastaFileName2 = getFastaFileName(index2);
        
        return fastaFileName1.equals(fastaFileName2);
    }
    
    public static int getKmerSize(Path indexFilePath) {
        return getKmerSize(indexFilePath.getName());
    }
    
    public static int getKmerSize(String indexFileName) {
        if(isKmerIndexIndexFile(indexFileName)) {
            int idx = indexFileName.lastIndexOf("." + PreprocessorConstants.KMER_INDEX_INDEX_FILENAME_EXTENSION);
            if(idx >= 0) {
                String part = indexFileName.substring(0, idx);
                int idx2 = part.lastIndexOf(".");
                if(idx2 >= 0) {
                    return Integer.parseInt(part.substring(idx2 + 1));
                }
            }
        } else if(isKmerIndexPartFile(indexFileName)) {
            int idx = indexFileName.lastIndexOf("." + PreprocessorConstants.KMER_INDEX_PART_FILENAME_EXTENSION);
            if(idx >= 0) {
                String part = indexFileName.substring(0, idx);
                int idx2 = part.lastIndexOf(".");
                if(idx2 >= 0) {
                    return Integer.parseInt(part.substring(idx2 + 1));
                }
            }
        }
        return -1;
    }
    
    public static int getIndexPartID(Path indexFilePath) {
        return getIndexPartID(indexFilePath.getName());
    }
    
    public static int getIndexPartID(String indexFileName) {
        int idx = indexFileName.lastIndexOf(".");
        if(idx >= 0) {
            String partID = indexFileName.substring(idx + 1);
            return Integer.parseInt(partID);
        }
        return -1;
    }
    
    public static Path[] getAllKmerIndexIndexFilePath(Configuration conf, String inputPathsCommaSeparated) throws IOException {
        return KmerIndexHelper.getAllKmerIndexIndexFilePath(conf, FileSystemHelper.makePathFromString(conf, FileSystemHelper.splitCommaSeparated(inputPathsCommaSeparated)));
    }
    
    public static Path[] getAllKmerIndexIndexFilePath(Configuration conf, Path inputPath) throws IOException {
        Path[] paths = new Path[1];
        paths[0] = inputPath;
        return KmerIndexHelper.getAllKmerIndexIndexFilePath(conf, paths);
    }
    
    public static Path[] getAllKmerIndexIndexFilePath(Configuration conf, String[] inputPaths) throws IOException {
        return KmerIndexHelper.getAllKmerIndexIndexFilePath(conf, FileSystemHelper.makePathFromString(conf, inputPaths));
    }
    
    public static Path[] getAllKmerIndexIndexFilePath(Configuration conf, Collection<String> inputPaths) throws IOException {
        return KmerIndexHelper.getAllKmerIndexIndexFilePath(conf, FileSystemHelper.makePathFromString(conf, inputPaths));
    }
    
    public static Path[] getAllKmerIndexIndexFilePath(Configuration conf, Path[] inputPaths) throws IOException {
        List<Path> inputFiles = new ArrayList<Path>();
        KmerIndexIndexPathFilter filter = new KmerIndexIndexPathFilter();
        
        for(Path path : inputPaths) {
            FileSystem fs = path.getFileSystem(conf);
            if(fs.exists(path)) {
                FileStatus status = fs.getFileStatus(path);
                if(status.isDir()) {
                    // check child
                    FileStatus[] entries = fs.listStatus(path);
                    for (FileStatus entry : entries) {
                        if(entry.isFile()) {
                            if (filter.accept(entry.getPath())) {
                                inputFiles.add(entry.getPath());
                            }
                        }
                    }
                } else if(status.isFile()) {
                    if (filter.accept(status.getPath())) {
                        inputFiles.add(status.getPath());
                    }
                }
            }
        }
        
        Path[] files = inputFiles.toArray(new Path[0]);
        return files;
    }
    
    public static Path[] getAllKmerIndexPartDataFilePath(Configuration conf, String inputPathsCommaSeparated) throws IOException {
        return KmerIndexHelper.getAllKmerIndexPartDataFilePath(conf, FileSystemHelper.makePathFromString(conf, FileSystemHelper.splitCommaSeparated(inputPathsCommaSeparated)));
    }
    
    public static Path[] getAllKmerIndexPartDataFilePath(Configuration conf, Path inputPath) throws IOException {
        Path[] paths = new Path[1];
        paths[0] = inputPath;
        return KmerIndexHelper.getAllKmerIndexPartDataFilePath(conf, paths);
    }
    
    public static Path[] getAllKmerIndexPartDataFilePath(Configuration conf, String[] inputPaths) throws IOException {
        return KmerIndexHelper.getAllKmerIndexPartDataFilePath(conf, FileSystemHelper.makePathFromString(conf, inputPaths));
    }
    
    public static Path[] getAllKmerIndexPartDataFilePath(Configuration conf, Collection<String> inputPaths) throws IOException {
        return KmerIndexHelper.getAllKmerIndexPartDataFilePath(conf, FileSystemHelper.makePathFromString(conf, inputPaths));
    }
    
    public static Path[] getAllKmerIndexPartDataFilePath(Configuration conf, Path[] inputPaths) throws IOException {
        List<Path> inputFiles = new ArrayList<Path>();
        KmerIndexPartPathFilter filter = new KmerIndexPartPathFilter();
        
        for(Path path : inputPaths) {
            FileSystem fs = path.getFileSystem(conf);
            if(fs.exists(path)) {
                FileStatus status = fs.getFileStatus(path);
                if(status.isDir()) {
                    if(filter.accept(path)) {
                        inputFiles.add(new Path(path, MapFile.DATA_FILE_NAME));
                    } else {
                        // check child
                        FileStatus[] entries = fs.listStatus(path);
                        for (FileStatus entry : entries) {
                            if(entry.isDir()) {
                                if (filter.accept(entry.getPath())) {
                                    inputFiles.add(new Path(entry.getPath(), MapFile.DATA_FILE_NAME));
                                }
                            }
                        }
                    }
                }
            }
        }
        
        return inputFiles.toArray(new Path[0]);
    }
    
    public static Path[] getKmerIndexPartFilePath(Configuration conf, Path inputPath) throws IOException {
        List<Path> inputFiles = new ArrayList<Path>();
        KmerIndexPartPathFilter filter = new KmerIndexPartPathFilter();
        
        Path indexDir = inputPath.getParent();
        
        FileSystem fs = indexDir.getFileSystem(conf);
        if(fs.exists(indexDir)) {
            FileStatus status = fs.getFileStatus(indexDir);
            if(status.isDir()) {
                // check child
                FileStatus[] entries = fs.listStatus(indexDir);
                for (FileStatus entry : entries) {
                    if(entry.isDir()) {
                        if (filter.accept(entry.getPath())) {
                            if(isSameKmerIndex(inputPath, entry.getPath())) {
                                inputFiles.add(entry.getPath());
                            }
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
