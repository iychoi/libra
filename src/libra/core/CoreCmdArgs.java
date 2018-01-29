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

import libra.common.cmdargs.CommandArgumentsBase;
import libra.core.common.CoreConfig;
import libra.core.common.RunMode;
import libra.core.common.WeightAlgorithm;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

/**
 *
 * @author iychoi
 */
public class CoreCmdArgs extends CommandArgumentsBase {
    
    public CoreCmdArgs() {
        
    }
    
    @Option(name = "-w", aliases = "--weight", usage = "specify weight algorithm")
    protected String weightAlgorithm = CoreConfig.DEFAULT_WEIGHT_ALGORITHM.name();

    public WeightAlgorithm getWeightAlgorithm() {
        return WeightAlgorithm.fromString(this.weightAlgorithm);
    }
    
    @Option(name = "-m", aliases = "--mode", usage = "specify run mode")
    protected String runMode = CoreConfig.DEFAULT_RUN_MODE.name();

    public RunMode getRunMode() {
        return RunMode.fromString(this.runMode);
    }
    
    @Option(name = "-t", aliases = "--tasks", usage = "specify number of tasks")
    protected int taskNum = CoreConfig.DEFAULT_TASKNUM;
    
    public int getTaskNum() {
        return this.taskNum;
    }
    
    @Option(name = "--no-histogram", usage = "do not use histogram, fixed-range partitioning will be used")
    protected boolean noHistogram = false;
    
    public boolean useHistogram() {
        return !this.noHistogram;
    }
    
    @Option(name = "-o", usage = "specify output path")
    private String outputPath = CoreConfig.DEFAULT_OUTPUT_PATH;
        
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
    
    public CoreConfig getCoreConfig() {
        CoreConfig config = new CoreConfig();
        
        config.setReportPath(this.reportfile);
        config.setWeightAlgorithm(getWeightAlgorithm());
        config.setRunMode(getRunMode());
        config.setTaskNum(this.taskNum);
        config.setUseHistogram(this.useHistogram());
        config.setPreprocessRootPath(this.preprocessOutputPath);
        config.setOutputPath(this.outputPath);
        return config;
    }
}
