/*
 * Copyright 2017 iychoi.
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

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

/**
 *
 * @author iychoi
 */
public class CryptoHelper {
    public static String sha1sum(String data) throws IOException {
        try {    
            MessageDigest messageDigest = MessageDigest.getInstance("SHA-1");
            messageDigest.update(data.getBytes());
            
            byte[] digest = messageDigest.digest();
            return toHexString(digest).toLowerCase();
        } catch (NoSuchAlgorithmException ex) {
            throw new IOException(ex);
        }
    }
    
    private static String toHexString(byte[] arr) {
        Formatter formatter = new Formatter();
        for (byte b : arr) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
    
    private static byte[] hexToBytes(String hex) {
        return hexToBytes(hex.toCharArray());
    }
    
    private static byte[] hexToBytes(char[] hex) {
        byte[] raw = new byte[hex.length / 2];
        for (int src = 0, dst = 0; dst < raw.length; ++dst) {
            int hi = Character.digit(hex[src++], 16);
            int lo = Character.digit(hex[src++], 16);
            if ((hi < 0) || (lo < 0)) {
                throw new IllegalArgumentException();
            }
            raw[dst] = (byte) (hi << 4 | lo);
        }
        return raw;
    }
}
