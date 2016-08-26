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
package libra.preprocess;

import java.util.ArrayList;
import java.util.List;
import libra.preprocess.stage1.KmerHistogramBuilder;
import libra.preprocess.stage2.KmerIndexBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class Preprocessor {
    private static final Log LOG = LogFactory.getLog(Preprocessor.class);
    
    private static int RUN_STAGE_1 = 0x01;
    private static int RUN_STAGE_2 = 0x02;
    
    private static boolean isHelpParam(String[] args) {
        if(args.length < 1 || 
                args[0].equalsIgnoreCase("-h") ||
                args[0].equalsIgnoreCase("--help")) {
            return true;
        }
        return false;
    }
    
    private static int checkRunStages(String[] args) {
        int runStages = 0;
        for(String arg : args) {
            if(arg.equalsIgnoreCase("stage1")) {
                runStages |= RUN_STAGE_1;
            } else if(arg.equalsIgnoreCase("stage2")) {
                runStages |= RUN_STAGE_2;
            }
        }
        
        if(runStages == 0) {
            runStages |= RUN_STAGE_1;
            runStages |= RUN_STAGE_2;
        }
        return runStages;
    }
    
    private static String[] removeRunStages(String[] args) {
        List<String> param = new ArrayList<String>();
        for(String arg : args) {
            if(!arg.equalsIgnoreCase("stage1") &&
                    !arg.equalsIgnoreCase("stage2")) {
                param.add(arg);
            }
        }
        
        return param.toArray(new String[0]);
    }
    
    public static int main2(String[] args) throws Exception {
        if(isHelpParam(args)) {
            printHelp();
            return 1;
        }
        
        int runStages = checkRunStages(args);
        String[] params = removeRunStages(args);
        
        int res = 0;
        try {       
            if((runStages & RUN_STAGE_1) == RUN_STAGE_1 && res == 0) {
                res = KmerHistogramBuilder.main2(params);
            }

            if((runStages & RUN_STAGE_2) == RUN_STAGE_2 && res == 0) {
                res = KmerIndexBuilder.main2(params);
            }
        } catch (Exception e) {
            LOG.error(e);
            res = 1;
        }
        
        return res;
    }
    
    public static void main(String[] args) throws Exception {
        int res = main2(args);
        System.exit(res);
    }

    private static void printHelp() {
        System.out.println("============================================================");
        System.out.println("Libra : Massive Comparative Analytic Tools for Metagenomics");
        System.out.println("Sample Preprocessor");
        System.out.println("============================================================");
        System.out.println("Usage :");
        System.out.println("> preprocessor [stage1|stage2|stage3] <arguments ...>");
        System.out.println();
        System.out.println("Stage :");
        System.out.println("> stage1");
        System.out.println("> \tBuild Kmer Histogram");
        System.out.println("> stage2");
        System.out.println("> \tBuild Kmer Indexes");
    }
}
