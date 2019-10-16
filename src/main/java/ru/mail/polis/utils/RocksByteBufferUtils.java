package ru.mail.polis.utils;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class RocksByteBufferUtils {

    /**
     * Used for transformation of array in ByteBuffer to unsigned byte array
     *
     * @param buffer source ByteBuffer
     * @return ByteBuffer
     */
    @NotNull
    public static ByteBuffer toUnsignedByteArray(@NotNull final ByteBuffer buffer) {
        final byte[] array = copyByteBuffer(buffer);
        for (int i = 0; i < array.length; i++) {
            final int uint = Byte.toUnsignedInt(array[i]);
            array[i] = (byte) (uint - Byte.MIN_VALUE);
        }
        return ByteBuffer.wrap(array);
    }

    /**
     * Used for transformation of unsigned byte array in ByteBuffer to byte array
     *
     * @param array source byte[] array
     * @return ByteBuffer
     */
    @NotNull
    public static ByteBuffer fromUnsignedByteArray(@NotNull final byte[] array) {
        final byte[] arrayCopy = Arrays.copyOf(array, array.length);
        for (int i = 0; i < arrayCopy.length; i++) {
            final int uint = Byte.toUnsignedInt(arrayCopy[i]);
            arrayCopy[i] = (byte) (uint + Byte.MIN_VALUE);
        }
        return ByteBuffer.wrap(arrayCopy);
    }

    /**
     * Return array from ByteBuffer that might be read-only.
     *
     * @param src source ByteBuffer
     * @return byte[] array
     */
    @NotNull
    public static byte[] copyByteBuffer(@NotNull final ByteBuffer src) {
        final byte[] array = new byte[src.remaining()];
        src.duplicate().get(array);
        return array;
    }
}
