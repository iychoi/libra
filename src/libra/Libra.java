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
import libra.distancematrix.DistanceMatrix;
import libra.group.Group;
import libra.preprocess.Preprocessor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class Libra {

    private static final Log LOG = LogFactory.getLog(Libra.class);
    
    private static boolean isHelpParam(String[] args) {
        if(args.length < 1 || 
                args[0].equalsIgnoreCase("-h") ||
                args[0].equalsIgnoreCase("--help")) {
            return true;
        }
        return false;
    }
    
    private static RunMode checkRunMode(String[] args) {
        for(String arg : args) {
            if(RunMode.isRunMode(arg)) {
                return RunMode.fromString(arg);
            }
        }
        
        return RunMode.PREPROCESS;
    }
    
    private static String[] removeRunMode(String[] args) {
        List<String> params = new ArrayList<String>();
        for(String arg : args) {
            if(!RunMode.isRunMode(arg)) {
                params.add(arg);
            }
        }
        
        return params.toArray(new String[0]);
    }
    
    public static void main(String[] args) throws Exception {
        if(isHelpParam(args)) {
            printHelp();
            return;
        } 
        
        RunMode runMode = checkRunMode(args);
        String[] params = removeRunMode(args);
        
        if(runMode == RunMode.PREPROCESS) {
            Preprocessor.main(params);
        } else if(runMode == RunMode.DISTANCEMATRIX) {
            DistanceMatrix.main(params);
        } else if(runMode == RunMode.GROUP) {
            Group.main(params);
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
        System.out.println("> distancematrix");
        System.out.println("> group");
    }
}
