package org.apache.arrow.table.util;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.holders.VarCharHolder;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Static utilities for building ValueHolders for variable-width vectors
 */
public class ValueHolderBuilder {

    private VarCharHolder holder;
    private BufferAllocator allocator;
    private Charset charset = StandardCharsets.UTF_8;
    private ArrowBuf buf;;
    /**
     * Private constructor to prevent instantiation
     */
    ValueHolderBuilder(VarCharHolder holder, BufferAllocator allocator, Charset charset) {
        this.holder = holder;
        this.charset = charset;
        this.allocator = allocator;
    }

    public void fillVarCharHolder(String value, int start) {
        byte[] bytes = value.getBytes(charset);
        if (buf != null) {
            buf.clear();
        } else {
            buf = allocator.buffer(bytes.length);
        }
        holder.start = start;
        holder.end = start + bytes.length;
        buf.writeBytes(bytes);
        holder.buffer = buf;
    }
}
