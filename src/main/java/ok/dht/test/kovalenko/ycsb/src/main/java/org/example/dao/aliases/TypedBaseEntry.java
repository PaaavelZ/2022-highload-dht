package org.example.dao.aliases;

import java.nio.ByteBuffer;

public record TypedBaseEntry(ByteBuffer key, ByteBuffer value) {
}
