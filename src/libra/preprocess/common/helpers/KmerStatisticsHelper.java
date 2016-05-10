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

import libra.preprocess.common.PreprocessorConstants;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author iychoi
 */
public class KmerStatisticsHelper {
    private final static String COUNTER_GROUP_NAME_LOGTFSQUARE = "KmerStatisticsLogTFSquare";
    
    public static String makeKmerStatisticsFileName(Path filePath) {
        return makeKmerStatisticsFileName(filePath.getName());
    }
    
    public static String makeKmerStatisticsFileName(String filename) {
        return filename + "." + PreprocessorConstants.KMER_STATISTICS_FILENAME_EXTENSION;
    }
    
    public static String getCounterGroupNameLogTFSquare() {
        return COUNTER_GROUP_NAME_LOGTFSQUARE;
    }
}
