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
package libra.common.helpers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import libra.common.fasta.FastaPathFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author iychoi
 */
public class FileSystemHelper {
    public static String makeCommaSeparated(Path[] strs) {
        if(strs == null) {
            return null;
        }
        
        StringBuilder sb = new StringBuilder();
        for(int i=0;i<strs.length;i++) {
            sb.append(strs[i].toString());
            if(i < strs.length - 1) {
                sb.append(",");
            }
        }
        return sb.toString();
    }
    
    public static String makeCommaSeparated(Collection<String> strs) {
        if(strs == null) {
            return null;
        }
        
        StringBuilder sb = new StringBuilder();
        int i=0;
        for(String str : strs) {
            sb.append(str);
            i++;
            
            if(i<strs.size()) {
                sb.append(",");
            }
        }
        return sb.toString();
    }
    
    public static String makeCommaSeparated(String[] strs) {
        if(strs == null) {
            return null;
        }
        
        StringBuilder sb = new StringBuilder();
        for(int i=0;i<strs.length;i++) {
            sb.append(strs[i]);
            if(i < strs.length - 1) {
                sb.append(",");
            }
        }
        return sb.toString();
    }
    
    public static String[] splitCommaSeparated(String comma_separated_input) {
        String[] inputs = comma_separated_input.split(",");
        return inputs;
    }
    
    public static Path[] makePathFromString(Configuration conf, Collection<String> pathStrings) throws IOException {
        return makePathFromString(conf, pathStrings.toArray(new String[0]));
    }
    
    public static Path[] makePathFromString(Configuration conf, String[] pathStrings) throws IOException {
        List<Path> paths = new ArrayList<Path>();
        for(String path : pathStrings) {
            if(hasWildcard(path)) {
                Path[] patharr = resolveWildcard(conf, path);
                for(Path pathentry : patharr) {
                    paths.add(pathentry);
                }
            } else {
                paths.add(new Path(path));
            }
        }
        return paths.toArray(new Path[0]);
    }
    
    public static String[] makeStringFromPath(Path[] paths) {
        String[] pathStrings = new String[paths.length];
        for(int i=0;i<paths.length;i++) {
            pathStrings[i] = paths[i].toString();
        }
        return pathStrings;
    }
    
    public static Path[] getAllFastaFilePath(Configuration conf, String inputPathsCommaSeparated) throws IOException {
        return getAllFastaFilePath(conf, makePathFromString(conf, splitCommaSeparated(inputPathsCommaSeparated)));
    }
    
    public static Path[] getAllFastaFilePath(Configuration conf, String[] inputPaths) throws IOException {
        return getAllFastaFilePath(conf, makePathFromString(conf, inputPaths));
    }
    
    public static Path[] getAllFastaFilePath(Configuration conf, Collection<String> inputPaths) throws IOException {
        return getAllFastaFilePath(conf, makePathFromString(conf, inputPaths));
    }
    
    public static Path[] getAllFastaFilePath(Configuration conf, Path[] inputPaths) throws IOException {
        List<Path> inputFiles = new ArrayList<Path>();
        FastaPathFilter filter = new FastaPathFilter();
        
        for(Path path : inputPaths) {
            FileSystem fs = path.getFileSystem(conf);
            FileStatus status = fs.getFileStatus(path);
            if(status.isDir()) {
                FileStatus[] entries = fs.listStatus(path);
                for(FileStatus entry : entries) {
                    if(filter.accept(entry.getPath())) {
                        inputFiles.add(entry.getPath());
                    }
                }
            } else {
                FileStatus entry = fs.getFileStatus(path);
                if(filter.accept(entry.getPath())) {
                    inputFiles.add(entry.getPath());
                }
            }
        }
        
        Path[] files = inputFiles.toArray(new Path[0]);
        return files;
    }
    
    public static boolean hasWildcard(String path) {
        if(path.indexOf("*") >= 0) {
            return true;
        }
        
        return false;
    }

    private static Path[] resolveWildcard(Configuration conf, String path) throws IOException {
        List<Path> paths = new ArrayList<Path>();
        
        int idxWildcard = path.lastIndexOf("*");
        if(idxWildcard >= 0) {
            String left = null;
            String right = path.substring(idxWildcard+1);
            
            String parentPath = path.substring(0, idxWildcard);
            int idxParent = parentPath.lastIndexOf("/");
            if(idxParent >= 0) {
                parentPath = parentPath.substring(0, idxParent);
                left = path.substring(idxParent+1, idxWildcard);
            } else {
                parentPath = "";
                left = path.substring(0, idxWildcard);
            }
            
            Path parent = new Path(parentPath);
            FileSystem fs = parent.getFileSystem(conf);
            FileStatus status = fs.getFileStatus(parent);
            if(status.isDir()) {
                FileStatus[] entries = fs.listStatus(parent);
                for(FileStatus entry : entries) {
                    if(!left.isEmpty()) {
                        if(!entry.getPath().getName().startsWith(left)) {
                            // skip
                            continue;
                        }
                    }
                    
                    if(!right.isEmpty()) {
                        if(!entry.getPath().getName().endsWith(right)) {
                            // skip
                            continue;
                        }
                    }
                    
                    paths.add(entry.getPath());
                }
            }
        }
        
        return paths.toArray(new Path[0]);
    }
}
