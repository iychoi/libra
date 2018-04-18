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

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import libra.common.helpers.PathHelper;
import libra.preprocess.common.PreprocessorConstants;
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
    
    public static String makeKmerHistogramDirPath(String rootPath) {
        return PathHelper.concatPath(rootPath, PreprocessorConstants.KMER_HISTOGRAM_DIRNAME);
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
}
