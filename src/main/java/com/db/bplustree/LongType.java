package com.db.bplustree;

import com.db.Block;

/**
 * Created by iceke on 16/7/20.
 */
public class LongType implements BPlusTreeType<Long>{


    @Override
    public int getSize() {
        return 8;
    }

    @Override
    public void write(Long data, Block block, int offset) {
        block.setLong(data, offset);
    }

    @Override
    public Long read(byte[] bytes, int offset) {
        return ((long)bytes[offset] << 56) | (((long)bytes[offset + 1] & 0xff) << 48)
                | (((long)bytes[offset + 2] & 0xff) << 40)
                | (((long)bytes[offset + 3] & 0xff) << 32)
                | (((long)bytes[offset + 4] & 0xff) << 24)
                | (((long)bytes[offset + 5] & 0xff) << 16)
                | (((long)bytes[offset + 6] & 0xff) << 8)
                | ((long)bytes[offset + 7] & 0xff);

    }

}
