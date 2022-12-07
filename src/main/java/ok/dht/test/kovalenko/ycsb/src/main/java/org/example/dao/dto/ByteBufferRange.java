package org.example.dao.dto;

import org.example.dao.utils.DaoUtils;

import java.nio.ByteBuffer;

public record ByteBufferRange(ByteBuffer from, ByteBuffer to) {

    public ByteBufferRange(ByteBuffer from, ByteBuffer to) {
        this.from = from == null ? DaoUtils.EMPTY_BYTEBUFFER : from;
        this.to = to;
    }
}
