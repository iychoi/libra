package libra;

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
import java.util.ArrayList;
import java.util.List;
import libra.core.Core;
import libra.preprocess.Preprocessor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class Libra {

    private static final Log LOG = LogFactory.getLog(Libra.class);
    
    private static int RUN_MODE_PREPROCESS = 0x00;
    private static int RUN_MODE_CORE = 0x01;
    
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
            if(arg.equalsIgnoreCase("preprocess")) {
                runMode = RUN_MODE_PREPROCESS;
            } else if(arg.equalsIgnoreCase("core")) {
                runMode = RUN_MODE_CORE;
            }
        }
        
        return runMode;
    }
    
    private static String[] removeRunMode(String[] args) {
        List<String> param = new ArrayList<String>();
        for(String arg : args) {
            if(!arg.equalsIgnoreCase("preprocess") && !arg.equalsIgnoreCase("core")) {
                param.add(arg);
            }
        }
        
        return param.toArray(new String[0]);
    }
    
    public static void main(String[] args) throws Exception {
        if(isHelpParam(args)) {
            printHelp();
            return;
        } 
        
        int runMode = checkRunMode(args);
        String[] params = removeRunMode(args);
        
        if(runMode == RUN_MODE_PREPROCESS) {
            Preprocessor.main(params);
        } else if(runMode == RUN_MODE_CORE) {
            Core.main(params);
        }
    }

    private static void printHelp() {
        System.out.println("============================================================");
        System.out.println("Libra : Massive Comparative Analytic Tools for Metagenomics");
        System.out.println("============================================================");
        System.out.println("Usage :");
        System.out.println("> libra <command> <arguments ...>");
        System.out.println();
        System.out.println("Commands :");
        System.out.println("> preprocess");
        System.out.println("> core");
    }
}
