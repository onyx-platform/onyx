/* Generated SBE (Simple Binary Encoding) message codec */
package onyx.serialization;

import org.agrona.MutableDirectBuffer;
import org.agrona.DirectBuffer;

@javax.annotation.Generated(value = {"onyx.serialization.BarrierEncoder"})
@SuppressWarnings("all")
public class BarrierEncoder
{
    public static final int BLOCK_LENGTH = 18;
    public static final int TEMPLATE_ID = 15;
    public static final int SCHEMA_ID = 1;
    public static final int SCHEMA_VERSION = 0;

    private final BarrierEncoder parentMessage = this;
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

    public BarrierEncoder wrap(final MutableDirectBuffer buffer, final int offset)
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

    public static int replicaVersionEncodingOffset()
    {
        return 0;
    }

    public static int replicaVersionEncodingLength()
    {
        return 8;
    }

    public static long replicaVersionNullValue()
    {
        return 0xffffffffffffffffL;
    }

    public static long replicaVersionMinValue()
    {
        return 0x0L;
    }

    public static long replicaVersionMaxValue()
    {
        return 0xfffffffffffffffeL;
    }

    public BarrierEncoder replicaVersion(final long value)
    {
        buffer.putLong(offset + 0, value, java.nio.ByteOrder.LITTLE_ENDIAN);
        return this;
    }


    public static int epochEncodingOffset()
    {
        return 8;
    }

    public static int epochEncodingLength()
    {
        return 8;
    }

    public static long epochNullValue()
    {
        return 0xffffffffffffffffL;
    }

    public static long epochMinValue()
    {
        return 0x0L;
    }

    public static long epochMaxValue()
    {
        return 0xfffffffffffffffeL;
    }

    public BarrierEncoder epoch(final long value)
    {
        buffer.putLong(offset + 8, value, java.nio.ByteOrder.LITTLE_ENDIAN);
        return this;
    }


    public static int destIdEncodingOffset()
    {
        return 16;
    }

    public static int destIdEncodingLength()
    {
        return 2;
    }

    public static int destIdNullValue()
    {
        return 65535;
    }

    public static int destIdMinValue()
    {
        return 0;
    }

    public static int destIdMaxValue()
    {
        return 65534;
    }

    public BarrierEncoder destId(final int value)
    {
        buffer.putShort(offset + 16, (short)value, java.nio.ByteOrder.LITTLE_ENDIAN);
        return this;
    }


    public static int payloadBytesId()
    {
        return 19;
    }

    public static String payloadBytesCharacterEncoding()
    {
        return "UTF-8";
    }

    public static String payloadBytesMetaAttribute(final MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case EPOCH: return "unix";
            case TIME_UNIT: return "nanosecond";
            case SEMANTIC_TYPE: return "";
        }

        return "";
    }

    public static int payloadBytesHeaderLength()
    {
        return 4;
    }

    public BarrierEncoder putPayloadBytes(final DirectBuffer src, final int srcOffset, final int length)
    {
        if (length > 1073741824)
        {
            throw new IllegalArgumentException("length > max value for type: " + length);
        }

        final int headerLength = 4;
        final int limit = parentMessage.limit();
        parentMessage.limit(limit + headerLength + length);
        buffer.putInt(limit, (int)length, java.nio.ByteOrder.LITTLE_ENDIAN);
        buffer.putBytes(limit + headerLength, src, srcOffset, length);

        return this;
    }

    public BarrierEncoder putPayloadBytes(final byte[] src, final int srcOffset, final int length)
    {
        if (length > 1073741824)
        {
            throw new IllegalArgumentException("length > max value for type: " + length);
        }

        final int headerLength = 4;
        final int limit = parentMessage.limit();
        parentMessage.limit(limit + headerLength + length);
        buffer.putInt(limit, (int)length, java.nio.ByteOrder.LITTLE_ENDIAN);
        buffer.putBytes(limit + headerLength, src, srcOffset, length);

        return this;
    }

    public BarrierEncoder payloadBytes(final String value)
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
        if (length > 1073741824)
        {
            throw new IllegalArgumentException("length > max value for type: " + length);
        }

        final int headerLength = 4;
        final int limit = parentMessage.limit();
        parentMessage.limit(limit + headerLength + length);
        buffer.putInt(limit, (int)length, java.nio.ByteOrder.LITTLE_ENDIAN);
        buffer.putBytes(limit + headerLength, bytes, 0, length);

        return this;
    }


    public String toString()
    {
        return appendTo(new StringBuilder(100)).toString();
    }

    public StringBuilder appendTo(final StringBuilder builder)
    {
        BarrierDecoder writer = new BarrierDecoder();
        writer.wrap(buffer, offset, BLOCK_LENGTH, SCHEMA_VERSION);

        return writer.appendTo(builder);
    }
}
