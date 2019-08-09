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
package libra.merge;

import libra.common.cmdargs.CommandArgumentsBase;
import libra.merge.common.MergeConfig;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

/**
 *
 * @author iychoi
 */
public class MergeCmdArgs extends CommandArgumentsBase {
    
    public MergeCmdArgs() {
        
    }
    
    @Option(name = "-t", aliases = "--tasks", usage = "specify number of tasks")
    protected int taskNum = MergeConfig.DEFAULT_TASKNUM;
    
    public int getTaskNum() {
        return this.taskNum;
    }
    
    @Option(name = "-o", usage = "specify output path")
    private String outputPath = MergeConfig.DEFAULT_OUTPUT_PATH;
        
    public String getOutputPath() {
        return this.outputPath;
    }
    
    @Argument(metaVar = "input-path", usage = "specify preprocess output path")
    private String preprocessOutputPath;

    public String getPreprocessOutputPath() {
        return this.preprocessOutputPath;
    }

    @Override
    public String toString() {
        return "path = " + this.preprocessOutputPath;
    }

    @Override
    public boolean checkValidity() {
        if(!super.checkValidity()) {
           return false;
        }
        
        if(this.taskNum < 0 ||
                this.preprocessOutputPath == null ||
                this.outputPath == null) {
            return false;
        }
        
        return true;
    }
    
    public MergeConfig getMergeConfig() {
        MergeConfig config = new MergeConfig();
        
        config.setReportPath(this.reportfile);
        config.setTaskNum(this.taskNum);
        config.setPreprocessRootPath(this.preprocessOutputPath);
        config.setOutputPath(this.outputPath);
        return config;
    }
}
