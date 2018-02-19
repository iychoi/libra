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
package libra.common.cmdargs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.kohsuke.args4j.Option;

/**
 *
 * @author iychoi
 */
public class CommandArgumentsBase {
    
    private static final Log LOG = LogFactory.getLog(CommandArgumentsBase.class);
    
    @Option(name = "-h", aliases = "--help", usage = "print help") 
    protected boolean help = false;

    @Option(name = "--report", usage = "specify a report file to be created")
    protected String reportfile;
    
    public boolean isHelp() {
        return this.help;
    }

    public boolean needReport() {
        return (reportfile != null);
    }
    
    public String getReportFilename() {
        return reportfile;
    }
    
    @Override
    public String toString() {
        return super.toString();
    }
    
    public boolean checkValidity() {
        return true;
    }
}
