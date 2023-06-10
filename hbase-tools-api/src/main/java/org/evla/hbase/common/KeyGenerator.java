package org.evla.hbase.common;

import org.apache.hadoop.hbase.util.Bytes;

import java.util.zip.CRC32;

public class KeyGenerator {

    public static void run(String input) {
        String[] splits = input.split("\n");
        for (String str : splits)
            generateKey(str);
    }

    public static void generateKey(String s) {
        byte[] bytes = KeyGenerator.generateKeyAsBytes(s);
        System.out.println("\"" + toStringBinary(bytes) + "\"");
    }

    public static String toStringBinary(final byte[] b) {
        if (b == null) {
            return "null";
        }
        return Bytes.toStringBinary(b, 0, b.length);
    }

    public static byte[] generateKeyAsBytes(String rowKey) {
        CRC32 crc32 = new CRC32();

        byte[] inputBytes = rowKey.getBytes();
        crc32.update(inputBytes);
        long salt = crc32.getValue();
        byte[] bytesFromSalt = Bytes.toBytes(salt);

        if (inputBytes.length < 4) {
            byte[] resultArray = new byte[4 + inputBytes.length];
            System.arraycopy(bytesFromSalt, 4, resultArray, 0, 4);
            System.arraycopy(inputBytes, 0, resultArray, 4, inputBytes.length);
            return resultArray;
        } else {
            byte[] resultArray = new byte[8 + inputBytes.length];
            System.arraycopy(bytesFromSalt, 4, resultArray, 0, 4);
            System.arraycopy(inputBytes, inputBytes.length - 2, resultArray, 4, 2);
            System.arraycopy(inputBytes, inputBytes.length - 4, resultArray, 6, 2);
            System.arraycopy(inputBytes, 0, resultArray, 8, inputBytes.length);

            return resultArray;
        }
    }
}
