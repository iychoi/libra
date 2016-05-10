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

import org.apache.hadoop.fs.Path;

/**
 *
 * @author iychoi
 */
public class MapReduceHelper {
    public static String getOutputNameFromMapReduceOutput(Path mapreduceOutputPath) {
        return getOutputNameFromMapReduceOutput(mapreduceOutputPath.getName());
    }
    
    public static String getOutputNameFromMapReduceOutput(String mapreduceOutputName) {
        int midx = mapreduceOutputName.indexOf("-m-");
        if(midx > 0) {
            return mapreduceOutputName.substring(0, midx);
        }
        
        int ridx = mapreduceOutputName.indexOf("-r-");
        if(ridx > 0) {
            return mapreduceOutputName.substring(0, ridx);
        }
        
        return mapreduceOutputName;
    }
    
    public static int getMapReduceID(Path mapreduceOutputName) {
        return getMapReduceID(mapreduceOutputName.getName());
    }
    
    public static int getMapReduceID(String mapreduceOutputName) {
        int midx = mapreduceOutputName.indexOf("-m-");
        if(midx > 0) {
            return Integer.parseInt(mapreduceOutputName.substring(midx + 3));
        }
        
        int ridx = mapreduceOutputName.indexOf("-r-");
        if(ridx > 0) {
            return Integer.parseInt(mapreduceOutputName.substring(ridx + 3));
        }
        
        return 0;
    }
    
    public static boolean isLogFiles(Path path) {
        if(path.getName().equals("_SUCCESS")) {
            return true;
        } else if(path.getName().equals("_logs")) {
            return true;
        }
        return false;
    }
    
    public static boolean isPartialOutputFiles(Path path) {
        if(path.getName().startsWith("part-r-")) {
            return true;
        } else if(path.getName().startsWith("part-m-")) {
            return true;
        }
        return false;
    }
}
