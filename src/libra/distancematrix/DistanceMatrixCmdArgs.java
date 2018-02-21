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
package libra.distancematrix;

import libra.common.cmdargs.CommandArgumentsBase;
import libra.distancematrix.common.DistanceMatrixConfig;
import libra.distancematrix.common.RunMode;
import libra.distancematrix.common.ScoreAlgorithm;
import libra.distancematrix.common.WeightAlgorithm;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

/**
 *
 * @author iychoi
 */
public class DistanceMatrixCmdArgs extends CommandArgumentsBase {
    
    public DistanceMatrixCmdArgs() {
        
    }
    
    @Option(name = "-w", aliases = "--weight", usage = "specify weight algorithm")
    protected String weightAlgorithm = DistanceMatrixConfig.DEFAULT_WEIGHT_ALGORITHM.name();

    public WeightAlgorithm getWeightAlgorithm() {
        return WeightAlgorithm.fromString(this.weightAlgorithm);
    }
    
    @Option(name = "-s", aliases = "--score", usage = "specify score algorithm")
    protected String scoreAlgorithm = DistanceMatrixConfig.DEFAULT_SCORE_ALGORITHM.name();
    
    public ScoreAlgorithm getScoreAlgorithm() {
        return ScoreAlgorithm.fromString(this.scoreAlgorithm);
    }
    
    @Option(name = "-m", aliases = "--mode", usage = "specify run mode")
    protected String runMode = DistanceMatrixConfig.DEFAULT_RUN_MODE.name();

    public RunMode getRunMode() {
        return RunMode.fromString(this.runMode);
    }
    
    @Option(name = "-t", aliases = "--tasks", usage = "specify number of tasks")
    protected int taskNum = DistanceMatrixConfig.DEFAULT_TASKNUM;
    
    public int getTaskNum() {
        return this.taskNum;
    }
    
    @Option(name = "--no-histogram", usage = "do not use histogram, fixed-range partitioning will be used")
    protected boolean noHistogram = false;
    
    public boolean useHistogram() {
        return !this.noHistogram;
    }
    
    @Option(name = "-o", usage = "specify output path")
    private String outputPath = DistanceMatrixConfig.DEFAULT_OUTPUT_PATH;
        
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
    
    public DistanceMatrixConfig getDistanceMatrixConfig() {
        DistanceMatrixConfig config = new DistanceMatrixConfig();
        
        config.setReportPath(this.reportfile);
        config.setWeightAlgorithm(getWeightAlgorithm());
        config.setScoreAlgorithm(getScoreAlgorithm());
        config.setRunMode(getRunMode());
        config.setTaskNum(this.taskNum);
        config.setUseHistogram(this.useHistogram());
        config.setPreprocessRootPath(this.preprocessOutputPath);
        config.setOutputPath(this.outputPath);
        return config;
    }
}
