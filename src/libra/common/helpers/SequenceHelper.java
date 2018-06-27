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

import java.io.IOException;
import java.math.BigInteger;

/**
 *
 * @author iychoi
 */
public class SequenceHelper {
    private static char[] ComplementCharLUT = {'T', ' ', 'G', ' ', ' ', ' ', 'C', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ',
    ' ', ' ', ' ', 'A', ' ', ' ', ' ', ' ', ' ', ' '};
    
    private static char[] convBitToCharLUT = {'A', 'C', 'G', 'T'};
    private static byte[] convCharToBitLUT = {0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
    private static boolean[] charAcceptLUT = {true, false, true, false, false, false, true, false, false, false, false, false, false, false, false, false,
    false, false, false, true, false, false, false, false, false, false};
    
    
    public static char getComplement(char ch) {
        return ComplementCharLUT[((byte)ch) - 'A'];
    }
    
    public static String getComplement(String sequence) {
        StringBuilder sb = new StringBuilder();
        for(int i=0;i<sequence.length();i++) {
            sb.append(getComplement(sequence.charAt(i)));
        }
        return sb.toString();
    }
    
    public static String getReverse(String sequence) {
        StringBuilder sb = new StringBuilder();
        for(int i=0;i<sequence.length();i++) {
            sb.append(sequence.charAt(sequence.length() - i - 1));
        }
        return sb.toString();
    }
    
    public static String getReverseComplement(String sequence) {
        StringBuilder sb = new StringBuilder();
        for(int i=0;i<sequence.length();i++) {
            sb.append(SequenceHelper.getComplement(sequence.charAt(sequence.length() - i - 1)));
        }
        return sb.toString();
    }
    
    private static char convBitToChar(byte bits) {
        return convBitToCharLUT[bits];
    }
    
    private static byte convCharToBit(char ch) {
        return convCharToBitLUT[((byte)ch) - 'A'];
    }
    
    public static int getCompressedSize(String sequence) {
        return getCompressedSize(sequence.length());
    }
    
    public static int getCompressedSize(int sequenceLen) {
        int bytes = sequenceLen / 4;
        if(sequenceLen % 4 != 0) {
            bytes++;
        }
        return bytes;
    }
    
    public static byte[] compress(String sequence) throws IOException {
        int sequenceLen = sequence.length();
        int compressedByteLen = sequenceLen / 4;
        if(sequenceLen % 4 != 0) {
            compressedByteLen++;
        }
        
        byte[] compressedArr = new byte[compressedByteLen];
        
        for(int i=0;i<sequenceLen / 4;i++) {
            char cha = sequence.charAt(i*4);
            char chb = sequence.charAt(i*4+1);
            char chc = sequence.charAt(i*4+2);
            char chd = sequence.charAt(i*4+3);
            
            byte a = (byte) (convCharToBit(cha) << 6);
            byte b = (byte) (convCharToBit(chb) << 4);
            byte c = (byte) (convCharToBit(chc) << 2);
            byte d = convCharToBit(chd);
            
            byte bits = (byte) (a | b | c | d);
            
            compressedArr[i] = bits;
        }
        
        int start = (sequenceLen / 4) * 4;
        int left = sequenceLen % 4;
        if(left > 0) {
            char cha = sequence.charAt(start);
            byte a = (byte) (convCharToBit(cha) << 6);
            byte b = 0;
            byte c = 0;
            if(left > 1) {
                char chb = sequence.charAt(start+1);
                b = (byte) (convCharToBit(chb) << 4);
                if(left > 2) {
                    char chc = sequence.charAt(start+2);
                    c = (byte) (convCharToBit(chc) << 2);
                }
            }
            
            byte bits = (byte) (a | b | c);
            compressedArr[sequenceLen / 4] = bits;
        }
        
        return compressedArr;
    }
    
    public static String decompress(byte[] compressed, int sequenceLen) {
        byte[] byteArr = new byte[sequenceLen];
        
        int curRead = 0;
        
        for(int i=0;i<compressed.length;i++) {
            byte bits = compressed[i];
            byte a = (byte)((bits >> 6) & 0x3);
            byte b = (byte)((bits >> 4) & 0x3);
            byte c = (byte)((bits >> 2) & 0x3);
            byte d = (byte)(bits & 0x3);
            
            char cha = convBitToChar(a);
            char chb = convBitToChar(b);
            char chc = convBitToChar(c);
            char chd = convBitToChar(d);

            if(curRead < sequenceLen) {
                byteArr[curRead] = (byte) cha;
                curRead++;
            }
            if(curRead < sequenceLen) {
                byteArr[curRead] = (byte) chb;
                curRead++;
            }
            if(curRead < sequenceLen) {
                byteArr[curRead] = (byte) chc;
                curRead++;
            }
            if(curRead < sequenceLen) {
                byteArr[curRead] = (byte) chd;
                curRead++;
            }
        }
        
        return new String(byteArr);
    }
    
    public static int convertToInteger(String sequence) {
        int iSequence = 0;
        int kmerSize = sequence.length();
        for(int i=0;i<kmerSize;i++) {
            char ch = sequence.charAt(i);
            if(ch == 'A') {
                iSequence += 0;
            } else if(ch == 'C') {
                iSequence += 1;
            } else if(ch == 'G') {
                iSequence += 2;
            } else if(ch == 'T') {
                iSequence += 3;
            }
            
            if(i < kmerSize - 1) {
                iSequence *= 4;
            }
        }
        
        return iSequence;
    }
    
    public static BigInteger convertToBigInteger(String sequence) {
        BigInteger biSequence = BigInteger.ZERO;
        int kmerSize = sequence.length();
        for(int i=0;i<kmerSize;i++) {
            char ch = sequence.charAt(i);
            if(ch == 'A') {
                biSequence = biSequence.add(BigInteger.valueOf(0));
            } else if(ch == 'C') {
                biSequence = biSequence.add(BigInteger.valueOf(1));
            } else if(ch == 'G') {
                biSequence = biSequence.add(BigInteger.valueOf(2));
            } else if(ch == 'T') {
                biSequence = biSequence.add(BigInteger.valueOf(3));
            }
            
            if(i < kmerSize - 1) {
                biSequence = biSequence.multiply(BigInteger.valueOf(4));
            }
        }
        
        return biSequence;
    }
    
    public static String convertToString(int iSequence, int kmerSize) {
        String str = "";
        for(int i=0;i<kmerSize;i++) {
            int idx = iSequence % 4;
            if(idx == 0) {
                str = "A" + str;
            } else if(idx == 1) {
                str = "C" + str;
            } else if(idx == 2) {
                str = "G" + str;
            } else if(idx == 3) {
                str = "T" + str;
            }
            iSequence /= 4;
        }
        
        return str;
    }
    
    public static String convertToString(BigInteger biSequence, int kmerSize) {
        String str = "";
        for(int i=0;i<kmerSize;i++) {
            int idx = biSequence.mod(BigInteger.valueOf(4)).intValue();
            if(idx == 0) {
                str = "A" + str;
            } else if(idx == 1) {
                str = "C" + str;
            } else if(idx == 2) {
                str = "G" + str;
            } else if(idx == 3) {
                str = "T" + str;
            }
            biSequence = biSequence.divide(BigInteger.valueOf(4));
        }
        
        return str;
    }
    
    public static int compareSequences(byte[] compressedSeq1, byte[] compressedSeq2) {
        for(int i=0;i<compressedSeq1.length;i++) {
            if(compressedSeq1[i] != compressedSeq2[i]) {
                byte bitsSeq1 = compressedSeq1[i];
                byte aSeq1 = (byte)((bitsSeq1 >> 6) & 0x3);
                byte bSeq1 = (byte)((bitsSeq1 >> 4) & 0x3);
                byte cSeq1 = (byte)((bitsSeq1 >> 2) & 0x3);
                byte dSeq1 = (byte)(bitsSeq1 & 0x3);
                
                byte bitsSeq2 = compressedSeq2[i];
                byte aSeq2 = (byte)((bitsSeq2 >> 6) & 0x3);
                byte bSeq2 = (byte)((bitsSeq2 >> 4) & 0x3);
                byte cSeq2 = (byte)((bitsSeq2 >> 2) & 0x3);
                byte dSeq2 = (byte)(bitsSeq2 & 0x3);
                
                if(aSeq1 - aSeq2 != 0) {
                    return aSeq1 - aSeq2;
                }
                
                if(bSeq1 - bSeq2 != 0) {
                    return bSeq1 - bSeq2;
                }
                
                if(cSeq1 - cSeq2 != 0) {
                    return cSeq1 - cSeq2;
                }
                
                if(dSeq1 - dSeq2 != 0) {
                    return dSeq1 - dSeq2;
                }
            }
        }
        return 0;
    }
    
    public static boolean isValidSequence(char ch) {
        if(ch >= 'A' && ch <= 'Z') {
            return charAcceptLUT[ch-'A'];
        } else {
            return false;
        }
    }
    
    public static boolean isValidSequence(String sequence) {
        for(int i=0;i<sequence.length();i++) {
            char ch = sequence.charAt(i);
            if(isValidSequence(ch)) {
                continue;
            } else {
                return false;
            }
        }
        return true;
    }
    
    public static String canonicalize(String sequence) {
        boolean determined = false;
        boolean use_rc = false;
        StringBuilder sb_rc = new StringBuilder();
        for(int i=0;i<sequence.length();i++) {
            char f_ch = sequence.charAt(i);
            char r_ch = sequence.charAt(sequence.length() - i - 1);
            char rc_ch = SequenceHelper.getComplement(r_ch);
            sb_rc.append(rc_ch);
            
            if(!determined) {
                if(rc_ch < f_ch) {
                    // determined
                    determined = true;
                    use_rc = true;
                } else if(rc_ch > f_ch) {
                    // determined
                    determined = true;
                    use_rc = false;
                    // short circuit
                    return sequence;
                }
            }
        }
        
        if(determined) {
            if(use_rc) {
                return sb_rc.toString();
            } else {
                return sequence;
            }
        } else {
            return sequence;
        }
    }
}
