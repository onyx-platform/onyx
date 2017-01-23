/* Generated SBE (Simple Binary Encoding) message codec */
package onyx.serialization;

import org.agrona.MutableDirectBuffer;
import org.agrona.DirectBuffer;

@javax.annotation.Generated(value = {"onyx.serialization.DynamicMapEncoder"})
@SuppressWarnings("all")
public class DynamicMapEncoder
{
    public static final int BLOCK_LENGTH = 2;
    public static final int TEMPLATE_ID = 25;
    public static final int SCHEMA_ID = 1;
    public static final int SCHEMA_VERSION = 0;

    private final DynamicMapEncoder parentMessage = this;
    private MutableDirectBuffer buffer;
    protected int offset;
    protected int limit;

    public int sbeBlockLength()
    {
        return BLOCK_LENGTH;
    }

    public int sbeTemplateId()
    {
        return TEMPLATE_ID;
    }

    public int sbeSchemaId()
    {
        return SCHEMA_ID;
    }

    public int sbeSchemaVersion()
    {
        return SCHEMA_VERSION;
    }

    public String sbeSemanticType()
    {
        return "";
    }

    public MutableDirectBuffer buffer()
    {
        return buffer;
    }

    public int offset()
    {
        return offset;
    }

    public DynamicMapEncoder wrap(final MutableDirectBuffer buffer, final int offset)
    {
        this.buffer = buffer;
        this.offset = offset;
        limit(offset + BLOCK_LENGTH);

        return this;
    }

    public int encodedLength()
    {
        return limit - offset;
    }

    public int limit()
    {
        return limit;
    }

    public void limit(final int limit)
    {
        this.limit = limit;
    }

    public static int keyTypeEncodingOffset()
    {
        return 0;
    }

    public static int keyTypeEncodingLength()
    {
        return 1;
    }

    public static byte keyTypeNullValue()
    {
        return (byte)-128;
    }

    public static byte keyTypeMinValue()
    {
        return (byte)-127;
    }

    public static byte keyTypeMaxValue()
    {
        return (byte)127;
    }

    public DynamicMapEncoder keyType(final byte value)
    {
        buffer.putByte(offset + 0, value);
        return this;
    }


    public static int valueTypeEncodingOffset()
    {
        return 1;
    }

    public static int valueTypeEncodingLength()
    {
        return 1;
    }

    public static byte valueTypeNullValue()
    {
        return (byte)-128;
    }

    public static byte valueTypeMinValue()
    {
        return (byte)-127;
    }

    public static byte valueTypeMaxValue()
    {
        return (byte)127;
    }

    public DynamicMapEncoder valueType(final byte value)
    {
        buffer.putByte(offset + 1, value);
        return this;
    }


    public static int keyId()
    {
        return 28;
    }

    public static String keyCharacterEncoding()
    {
        return "UTF-8";
    }

    public static String keyMetaAttribute(final MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case EPOCH: return "unix";
            case TIME_UNIT: return "nanosecond";
            case SEMANTIC_TYPE: return "";
        }

        return "";
    }

    public static int keyHeaderLength()
    {
        return 2;
    }

    public DynamicMapEncoder putKey(final DirectBuffer src, final int srcOffset, final int length)
    {
        if (length > 65534)
        {
            throw new IllegalArgumentException("length > max value for type: " + length);
        }

        final int headerLength = 2;
        final int limit = parentMessage.limit();
        parentMessage.limit(limit + headerLength + length);
        buffer.putShort(limit, (short)length, java.nio.ByteOrder.LITTLE_ENDIAN);
        buffer.putBytes(limit + headerLength, src, srcOffset, length);

        return this;
    }

    public DynamicMapEncoder putKey(final byte[] src, final int srcOffset, final int length)
    {
        if (length > 65534)
        {
            throw new IllegalArgumentException("length > max value for type: " + length);
        }

        final int headerLength = 2;
        final int limit = parentMessage.limit();
        parentMessage.limit(limit + headerLength + length);
        buffer.putShort(limit, (short)length, java.nio.ByteOrder.LITTLE_ENDIAN);
        buffer.putBytes(limit + headerLength, src, srcOffset, length);

        return this;
    }

    public DynamicMapEncoder key(final String value)
    {
        final byte[] bytes;
        try
        {
            bytes = value.getBytes("UTF-8");
        }
        catch (final java.io.UnsupportedEncodingException ex)
        {
            throw new RuntimeException(ex);
        }

        final int length = bytes.length;
        if (length > 65534)
        {
            throw new IllegalArgumentException("length > max value for type: " + length);
        }

        final int headerLength = 2;
        final int limit = parentMessage.limit();
        parentMessage.limit(limit + headerLength + length);
        buffer.putShort(limit, (short)length, java.nio.ByteOrder.LITTLE_ENDIAN);
        buffer.putBytes(limit + headerLength, bytes, 0, length);

        return this;
    }

    public static int valueId()
    {
        return 29;
    }

    public static String valueCharacterEncoding()
    {
        return "UTF-8";
    }

    public static String valueMetaAttribute(final MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case EPOCH: return "unix";
            case TIME_UNIT: return "nanosecond";
            case SEMANTIC_TYPE: return "";
        }

        return "";
    }

    public static int valueHeaderLength()
    {
        return 2;
    }

    public DynamicMapEncoder putValue(final DirectBuffer src, final int srcOffset, final int length)
    {
        if (length > 65534)
        {
            throw new IllegalArgumentException("length > max value for type: " + length);
        }

        final int headerLength = 2;
        final int limit = parentMessage.limit();
        parentMessage.limit(limit + headerLength + length);
        buffer.putShort(limit, (short)length, java.nio.ByteOrder.LITTLE_ENDIAN);
        buffer.putBytes(limit + headerLength, src, srcOffset, length);

        return this;
    }

    public DynamicMapEncoder putValue(final byte[] src, final int srcOffset, final int length)
    {
        if (length > 65534)
        {
            throw new IllegalArgumentException("length > max value for type: " + length);
        }

        final int headerLength = 2;
        final int limit = parentMessage.limit();
        parentMessage.limit(limit + headerLength + length);
        buffer.putShort(limit, (short)length, java.nio.ByteOrder.LITTLE_ENDIAN);
        buffer.putBytes(limit + headerLength, src, srcOffset, length);

        return this;
    }

    public DynamicMapEncoder value(final String value)
    {
        final byte[] bytes;
        try
        {
            bytes = value.getBytes("UTF-8");
        }
        catch (final java.io.UnsupportedEncodingException ex)
        {
            throw new RuntimeException(ex);
        }

        final int length = bytes.length;
        if (length > 65534)
        {
            throw new IllegalArgumentException("length > max value for type: " + length);
        }

        final int headerLength = 2;
        final int limit = parentMessage.limit();
        parentMessage.limit(limit + headerLength + length);
        buffer.putShort(limit, (short)length, java.nio.ByteOrder.LITTLE_ENDIAN);
        buffer.putBytes(limit + headerLength, bytes, 0, length);

        return this;
    }


    public String toString()
    {
        return appendTo(new StringBuilder(100)).toString();
    }

    public StringBuilder appendTo(final StringBuilder builder)
    {
        DynamicMapDecoder writer = new DynamicMapDecoder();
        writer.wrap(buffer, offset, BLOCK_LENGTH, SCHEMA_VERSION);

        return writer.appendTo(builder);
    }
}
