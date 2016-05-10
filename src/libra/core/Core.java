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
package libra.core;

import java.util.ArrayList;
import java.util.List;
import libra.common.cmdargs.CommandArgumentsParser;
import libra.core.commom.CoreConfig;
import libra.core.kmersimilarity_m.KmerSimilarityMap;
import libra.core.kmersimilarity_r.KmerSimilarityReduce;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author iychoi
 */
public class Core extends Configured implements Tool {
    private static final Log LOG = LogFactory.getLog(Core.class);
    
    private static int RUN_MODE_MAP = 0x00;
    private static int RUN_MODE_REDUCE = 0x01;
    
    private static boolean isHelpParam(String[] args) {
        if(args.length < 1 || 
                args[0].equalsIgnoreCase("-h") ||
                args[0].equalsIgnoreCase("--help")) {
            return true;
        }
        return false;
    }
    
    private static int checkRunMode(String[] args) {
        int runMode = 0;
        for(String arg : args) {
            if(arg.equalsIgnoreCase("map")) {
                runMode = RUN_MODE_MAP;
            } else if(arg.equalsIgnoreCase("reduce")) {
                runMode = RUN_MODE_REDUCE;
            }
        }
        
        return runMode;
    }
    
    private static String[] removeRunMode(String[] args) {
        List<String> param = new ArrayList<String>();
        for(String arg : args) {
            if(!arg.equalsIgnoreCase("map") && !arg.equalsIgnoreCase("reduce")) {
                param.add(arg);
            }
        }
        
        return param.toArray(new String[0]);
    }
    
    @Override
    public int run(String[] args) throws Exception {
        if(isHelpParam(args)) {
            printHelp();
            return 1;
        }
        
        int runMode = checkRunMode(args);
        String[] params = removeRunMode(args);
        
        CommandArgumentsParser<CoreCmdArgs> parser = new CommandArgumentsParser<CoreCmdArgs>();
        CoreCmdArgs cmdParams = new CoreCmdArgs();
        if(!parser.parse(params, cmdParams)) {
            printHelp();
            return 1;
        }

        CoreConfig cConfig = cmdParams.getCoreConfig();
        
        int res = 1;
        if(runMode == RUN_MODE_MAP) {
            KmerSimilarityMap similarity = new KmerSimilarityMap();
            res = similarity.run(cConfig);
        } else if(runMode == RUN_MODE_REDUCE) {
            KmerSimilarityReduce similarity = new KmerSimilarityReduce();
            res = similarity.run(cConfig);
        }

        return res;
    }
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Core(), args);
        System.exit(res);
    }

    private static void printHelp() {
        System.out.println("============================================================");
        System.out.println("Libra : Massive Comparative Analytic Tools for Metagenomics");
        System.out.println("Similarity Computer");
        System.out.println("============================================================");
        System.out.println("Usage :");
        System.out.println("> core [map|reduce] <arguments ...>");
        System.out.println();
        System.out.println("Mode :");
        System.out.println("> map");
        System.out.println("> \tCompute similarity using mappers");
        System.out.println("> reduce");
        System.out.println("> \tCompute similarity using reducers");
    }
}
