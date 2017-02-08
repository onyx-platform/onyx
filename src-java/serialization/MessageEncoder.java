/* Generated SBE (Simple Binary Encoding) message codec */
package onyx.serialization;

import org.agrona.MutableDirectBuffer;
import org.agrona.DirectBuffer;

@javax.annotation.Generated(value = {"onyx.serialization.MessageEncoder"})
@SuppressWarnings("all")
public class MessageEncoder
{
    public static final int BLOCK_LENGTH = 10;
    public static final int TEMPLATE_ID = 1;
    public static final int SCHEMA_ID = 1;
    public static final int SCHEMA_VERSION = 0;

    private final MessageEncoder parentMessage = this;
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

    public MessageEncoder wrap(final MutableDirectBuffer buffer, final int offset)
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

    public MessageEncoder replicaVersion(final long value)
    {
        buffer.putLong(offset + 0, value, java.nio.ByteOrder.LITTLE_ENDIAN);
        return this;
    }


    public static int destIdEncodingOffset()
    {
        return 8;
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

    public MessageEncoder destId(final int value)
    {
        buffer.putShort(offset + 8, (short)value, java.nio.ByteOrder.LITTLE_ENDIAN);
        return this;
    }


    private final SegmentsEncoder segments = new SegmentsEncoder();

    public static long segmentsId()
    {
        return 4;
    }

    public SegmentsEncoder segmentsCount(final int count)
    {
        segments.wrap(parentMessage, buffer, count);
        return segments;
    }

    public static class SegmentsEncoder
    {
        private static final int HEADER_SIZE = 4;
        private final GroupSizeEncodingEncoder dimensions = new GroupSizeEncodingEncoder();
        private MessageEncoder parentMessage;
        private MutableDirectBuffer buffer;
        private int count;
        private int index;
        private int offset;

        public void wrap(
            final MessageEncoder parentMessage, final MutableDirectBuffer buffer, final int count)
        {
            if (count < 0 || count > 65534)
            {
                throw new IllegalArgumentException("count outside allowed range: count=" + count);
            }

            this.parentMessage = parentMessage;
            this.buffer = buffer;
            dimensions.wrap(buffer, parentMessage.limit());
            dimensions.blockLength((int)0);
            dimensions.numInGroup((int)count);
            index = -1;
            this.count = count;
            parentMessage.limit(parentMessage.limit() + HEADER_SIZE);
        }

        public static int sbeHeaderSize()
        {
            return HEADER_SIZE;
        }

        public static int sbeBlockLength()
        {
            return 0;
        }

        public SegmentsEncoder next()
        {
            if (index + 1 >= count)
            {
                throw new java.util.NoSuchElementException();
            }

            offset = parentMessage.limit();
            parentMessage.limit(offset + sbeBlockLength());
            ++index;

            return this;
        }

        public static int segmentBytesId()
        {
            return 5;
        }

        public static String segmentBytesCharacterEncoding()
        {
            return "UTF-8";
        }

        public static String segmentBytesMetaAttribute(final MetaAttribute metaAttribute)
        {
            switch (metaAttribute)
            {
                case EPOCH: return "unix";
                case TIME_UNIT: return "nanosecond";
                case SEMANTIC_TYPE: return "";
            }

            return "";
        }

        public static int segmentBytesHeaderLength()
        {
            return 4;
        }

        public SegmentsEncoder putSegmentBytes(final DirectBuffer src, final int srcOffset, final int length)
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

        public SegmentsEncoder putSegmentBytes(final byte[] src, final int srcOffset, final int length)
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

        public SegmentsEncoder segmentBytes(final String value)
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
    }


    public String toString()
    {
        return appendTo(new StringBuilder(100)).toString();
    }

    public StringBuilder appendTo(final StringBuilder builder)
    {
        MessageDecoder writer = new MessageDecoder();
        writer.wrap(buffer, offset, BLOCK_LENGTH, SCHEMA_VERSION);

        return writer.appendTo(builder);
    }
}
