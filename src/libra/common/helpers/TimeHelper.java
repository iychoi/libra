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

import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 * @author iychoi
 */
public class TimeHelper {
    public static long getCurrentTime() {
        return System.currentTimeMillis();
    }
    
    public static String getDiffTimeString(long begin, long end) {
        long diff = end - begin;
        long remain = diff;
        
        int msec = (int) (remain % 1000);
        remain /= 1000;
        int sec = (int) (remain % 60);
        remain /= 60;
        int min = (int) (remain % 60);
        remain /= 60;
        int hour = (int) (remain);
        
        return hour + "h " + min + "m " + sec + "s";
    }
    
    public static String getTimeString(long time) {
        Date date = new Date(time);
        Format format = new SimpleDateFormat("yyyy MM dd HH:mm:ss");
        return format.format(date);
    }
}
