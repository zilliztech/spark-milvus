package zilliztech.spark.milvus;

import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

public class SparseVectorConvertTest {

    @Test
    public void parse() throws Exception {
        byte[] bytes = new byte[]{1, 0, 0, 0, -69, -43, -1, 62, 2, 0, 0, 0, 94, -23, 73, 63, 3, 0, 0, 0, 114, 28, -103, 62, 7, 0, 0, 0, 55, 21, -83, 61, 8, 0, 0, 0, -110, -109, 59, 63, 9, 0, 0, 0, -63, -15, 75, 63, 10, 0, 0, 0, -127, 74, 113, 62, 11, 0, 0, 0, -33, -101, 45, 63, 12, 0, 0, 0, -50, -47, 122, 62, 13, 0, 0, 0, -53, 35, -125, 62, 14, 0, 0, 0, 45, -44, -8, 61};
        SortedMap<Integer, Float> map = bytesToSparseVector(bytes);
        for (int key: map.keySet()) {
            System.out.println(key + " -> " + map.get(key));
        }
    }

    public static byte[] serializeSortedMap(SortedMap<Long, Float> map) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(map);
            oos.flush();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Error serializing SortedMap", e);
        }
    }

    public static SortedMap<Integer, Float> bytesToSparseVector(byte[] bytes) {
        int length = bytes.length;
        int p = 0;
        SortedMap<Integer, Float> map = new TreeMap<>();
        while (p < length) {
            ByteBuffer buffer = ByteBuffer.wrap(bytes, p, 4);
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            int k = buffer.getInt();
            p = p + 4;

            buffer = ByteBuffer.wrap(bytes, p, 4);
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            float v = buffer.getFloat();
            p = p + 4;

            map.put(k, v);
        }
        return map;
    }
}
