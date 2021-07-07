import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * 自动压缩，维持原数组特性。增加数据可以使用 add 方法，获取数据可以使用 get。同时还提供一个类似 Iterator 的实现
 * ，但其支持指定偏例起始与范围。
 *
 * innovated by Zhou Yao @ 2021.06.18
 */
public class ZippedLongArray {
    // 每个桶需要存的数
    private static int SHARD_SIZE = 64;

    // 每次扩展桶的个数，可依据数据量的大小调整，提升初始化效率
    private static int SHARD_EXPAND_COUNT = 128;

    private int shardCount = SHARD_EXPAND_COUNT;

    private int size = 0;

    // 当前桶指针
    private int shardCursor = 0;

    // 当前桶内可用指针，由于肯定小于 SHARD_SIZE * 8，所以可以是 short
    private short lastNumPos = 0;

    // 数据存储所在的二维数组
    private byte[][] shards;

    public ZippedLongArray(int count) {
        shardCount = count / SHARD_SIZE + 1;
        shards = new byte[shardCount][];
    }

    public ZippedLongArray() {
        shards = new byte[shardCount][];
    }

    /**
     * <b>按增序</b>进行添加, 压缩效果最佳，由于占用了高 4 位（前 3 位表示数据长度，后 1 位表示正负），因此数值不能超过 0xf0L << 55，即除
     * 开符号位后高 4 位不能使用。
     *
     * @param value 值
     */
    public void add(long value) {
        if (Math.abs(value) >= 0xf0L << 55) {
            throw new IllegalArgumentException("Number too large, do not support bigger(equal) than 0xf0L << 55");
        }

        if (shards[shardCursor] == null) {
            // 初始化的数组大小肯定够存对应的数，后面压缩后再缩减整个数组
            shards[shardCursor] = new byte[SHARD_SIZE * 8];
        }

        ByteBuffer buf = ByteBuffer.wrap(shards[shardCursor]);
        buf.position(lastNumPos);

        byte[] zipped = deflate(value);

        // 对于桶中除第一个数，仅存偏移量，可以进一步压缩数据
        if (lastNumPos > 0) {
            zipped = deflate(value - get(shardCursor * SHARD_SIZE));
        }

        // 如果当前桶放不下，创建一个新桶
        if (size - shardCursor * SHARD_SIZE + 1 > SHARD_SIZE) {
            byte[] zippedArray = new byte[buf.position()];
            System.arraycopy(buf.array(), 0, zippedArray, 0, zippedArray.length);
            shards[shardCursor++] = zippedArray;

            if (shardCursor >= shardCount) {
                expandShards();
            }

            buf = ByteBuffer.allocate(SHARD_SIZE * 8);
            shards[shardCursor] = buf.array();

            lastNumPos = 0;

            // 新开桶后偏移量不再起作用
            zipped = deflate(value);
        }

        buf.put(zipped);
        lastNumPos = (short) buf.position();

        size++;
    }

    public long get(int ix) {
        // 首先寻找 shard, 由于每个桶存储固定数量的数字，因此可以直接映射
        int i = ix / SHARD_SIZE;

        // 剩下的为需要线性查找的偏移量
        ix %= SHARD_SIZE;

        byte[] shard = shards[i];

        // 找到对应数据的偏移量
        long offset = 0;
        if (ix > 0) {
            int len = (Byte.toUnsignedInt(shard[0]) >>> 5);
            offset = inflate(shards[i], 0, len);
        }

        int numPos = 0;
        while (ix > 0) {
            int len = (Byte.toUnsignedInt(shard[numPos]) >>> 5);

            numPos += len;
            ix -= 1;
        }

        int len = (Byte.toUnsignedInt(shard[numPos]) >>> 5);

        return offset + inflate(shards[i], numPos, len);
    }

    private void expandShards() {
        // 对桶进行扩容
        shardCount += SHARD_EXPAND_COUNT;

        byte[][] newShards = new byte[shardCount][];
        System.arraycopy(shards, 0, newShards, 0, shards.length);
        shards = newShards;
    }

    private static byte[] deflate(long num) {
        int negative = num < 0 ? 0x01 : 0x00;

        // 存储只使用正数
        num = (num < 0) ? -num : num;

        // 将数字有效的字节提取出来
        byte[] tp = new byte[8];
        int n = 0;
        do {
            tp[n++] = (byte) num;
        } while (((num & ~0x0f) | (num >>= 8)) > 0);

        // 前三位表示占用位数
        tp[n - 1] = (byte)(tp[n - 1] | (n) << 5);

        // 第四位表示正数或者负数
        tp[n - 1] = (byte)(tp[n - 1] | negative << 4);

        byte[] zipped = new byte[n];
        for (int i = n; i > 0; i--) {
            zipped[n - i] = tp[i - 1];
        }

        return zipped;
    }

    private static long inflate(byte[] bag) {
        return inflate(bag, 0, bag.length);
    }

    private static long inflate(byte[] shard, int numPos, int len) {
        long data = 0;

        // 将数据放入以 long 表示的栈数组
        for (int i = 0; i < len; i++) {
            // & 0xff 表示转换成无符号数
            data |= (long) (0xff & shard[numPos + i]) << (len - i - 1) * 8;
        }

        // 查看符号位
        long negative = data & (0x10L << (len - 1) * 8);

        // 将占用位数据清零
        data &= ~(0xf0L << (len - 1) * 8);

        return negative > 0 ? -data : data;
    }

    public int getShardCursor() {
        return shardCursor;
    }

    public int getShardSize() {
        return SHARD_SIZE;
    }

    public long getTotalBytesUsed() {
        long bytes = Arrays.stream(shards).mapToLong(t -> {
            return t == null ? 0 : t.length;
        }).sum();

        // 一个引用占用 16 字节
        return shards.length * 16L + bytes;
    }

    public int size() {
        return size;
    }

    public ZippedIterator getIterator(int startIndex, int count) {
        return new ZippedIterator(startIndex, count);
    }

    public class ZippedIterator {
        private int numPos;

        private int shardIndex;

        private long curOffset;

        private int count;

        private ZippedIterator(int ix, int count) {
            if (ix + count > size) {
                throw new IndexOutOfBoundsException("end index overflow:" + (ix + count) + " of " + size);
            }

            // 首先寻找 shard, 由于每个桶存储固定数量的数字，因此可以直接映射
            shardIndex = ix / SHARD_SIZE;

            // 剩下的为需要线性查找的偏移量
            ix %= SHARD_SIZE;

            // 找到对应数据的偏移量
            byte[] shard = shards[shardIndex];
            curOffset = inflate(shard, 0, (0xff & shard[0]) >>> 5);

            while (ix > 0) {
                int len = (0xff & shard[numPos]) >>> 5;

                numPos += len;
                ix -= 1;
            }

            this.count = count;
        }

        public long nextLong() {
            byte[] shard = shards[shardIndex];
            int len = (0xff & shard[numPos]) >>> 5;

            // 每个桶第一个数据不需要增加初始值，后面的需要，因为只存偏移量
            long data = numPos > 0 ? curOffset + inflate(shard, numPos, len) : inflate(shard, numPos, len);

            numPos += len;

            if (numPos >= shard.length) {
                shardIndex++;
                numPos = 0;

                // 找到对应数据的偏移量
                shard = shards[shardIndex];
                curOffset = inflate(shard, 0, (0xff & shard[0]) >>> 5);
            }

            count--;

            return data;
        }

        public boolean hasNext() {
            return count > 0;
        }

    }
}
