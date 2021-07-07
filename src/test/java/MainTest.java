import org.apache.commons.lang.math.RandomUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.junit.Test;

import java.util.*;

public class MainTest {
    @Test
    public void testAll() {
        // 200 millis numbers to test
        int size = 20 * 1000 * 1000;

        long[] longArray = new long[size];
        int[] intArray = new int[size];

        for (int i = 0; i < size; i++) {
            longArray[i] = RandomUtils.nextLong() % size;
            intArray[i] = RandomUtils.nextInt() % size;
        }

        // 如果业务对顺序不敏感，排序后压缩效果非常好，当然不排序也可以压缩
        Arrays.sort(longArray);
        Arrays.sort(intArray);

        long originalBytes = RamUsageEstimator.sizeOf(longArray);
        System.out.println("RAM Long Original: " + RamUsageEstimator.humanReadableUnits(originalBytes));

        ZippedLongArray zippedLongAry = new ZippedLongArray();
        for (int i = 0; i < longArray.length; i++) {
            zippedLongAry.add(longArray[i]);
        }

        long zippedBytes = zippedLongAry.getTotalBytesUsed();
        System.out.println("RAM Long Zipped: " + RamUsageEstimator.humanReadableUnits(zippedBytes));

        long cur = System.currentTimeMillis();
        int count = 0;
        while (System.currentTimeMillis() < cur + 5000) {
            int ix = RandomUtils.nextInt() % longArray.length;

            if (longArray[ix] != zippedLongAry.get(ix)) {
                throw new IllegalStateException("test failed");
            }

            count++;
        }

        System.out.println("OK+, " + count + " times tested, avg TPS:" + count / 5);

        originalBytes = RamUsageEstimator.sizeOf(intArray);
        System.out.println("RAM Integer Original: " + RamUsageEstimator.humanReadableUnits(originalBytes));

        ZippedLongArray zippedIntAry = new ZippedLongArray();
        for (int i = 0; i < intArray.length; i++) {
            zippedIntAry.add(intArray[i]);
        }

        zippedBytes = zippedIntAry.getTotalBytesUsed();
        System.out.println("RAM Integer Zipped: " + RamUsageEstimator.humanReadableUnits(zippedBytes));

        cur = System.currentTimeMillis();
        count = 0;
        while (System.currentTimeMillis() < cur + 5000) {
            int ix = RandomUtils.nextInt() % longArray.length;

            if (intArray[ix] != zippedIntAry.get(ix)) {
                throw new IllegalStateException("test failed");
            }

            count++;
        }

        System.out.println("OK+, " + count + " times tested, avg TPS:" + count / 5);

        cur = System.currentTimeMillis();
        while (System.currentTimeMillis() < cur + 5000) {
            int ix = RandomUtils.nextInt() % longArray.length;

            ZippedLongArray.ZippedIterator longIter = zippedLongAry.getIterator(ix, zippedLongAry.size() - ix);
            int i = ix;
            while (longIter.hasNext()) {
                if (longArray[i++] != longIter.nextLong()) {
                    throw new IllegalStateException("test failed");
                }
            }

            ZippedLongArray.ZippedIterator intIter = zippedIntAry.getIterator(ix, zippedIntAry.size() - ix);
            i = ix;
            while (intIter.hasNext()) {
                if (intArray[i++] != intIter.nextLong()) {
                    throw new IllegalStateException("test failed");
                }
            }
        }

        System.out.println("OK+, batch fetch tested");
    }
}
