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
import libra.distancematrix.DistanceMatrix;
import libra.group.Group;
import libra.merge.Merge;
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
    
    public static void main(String[] args) throws Exception {
        if(isHelpParam(args)) {
            printHelp();
            return;
        } 
        
        // detect run mode and 
        // take it out from arguments to make further parsing simple
        RunMode runMode = null;
        String[] params = new String[args.length-1];
        int indexParam = 0;
        
        for(int i=0;i<args.length;i++) {
            String arg = args[i];
            if(runMode == null) {
                RunMode mode = RunMode.fromString(arg);
                if(mode != null) {
                    runMode = mode;
                    continue;
                }
            }
            
            params[indexParam] = arg;
            indexParam++;
        }
        
        // call a stage specified
        if(null == runMode) {
            System.out.println("Command is not given");
        } else {
            switch (runMode) {
                case PREPROCESS:
                    Preprocessor.main(params);
                    break;
                case DISTANCEMATRIX:
                    DistanceMatrix.main(params);
                    break;
                case GROUP:
                    Group.main(params);
                    break;
                case MERGE:
                    Merge.main(params);
                    break;
                default:
                    break;
            }
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
        System.out.println("> merge");
    }
}
