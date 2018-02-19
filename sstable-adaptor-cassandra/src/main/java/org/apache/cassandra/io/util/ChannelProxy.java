package org.apache.cassandra.io.util;

import org.apache.cassandra.utils.concurrent.RefCounted;
import org.apache.cassandra.utils.concurrent.SharedCloseableImpl;

import java.nio.ByteBuffer;

public abstract class ChannelProxy extends SharedCloseableImpl
{
    public ChannelProxy(RefCounted.Tidy tidy)
    {
        super(tidy);
    }

    protected ChannelProxy(SharedCloseableImpl copy)
    {
        super(copy);
    }

    protected abstract long size();
    public abstract int read(ByteBuffer buffer, long position);
    public abstract String filePath();
    public void reopenInputStream()
    {
    }
}
