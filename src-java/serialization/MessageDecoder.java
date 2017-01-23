/* Generated SBE (Simple Binary Encoding) message codec */
package onyx.serialization;

import org.agrona.MutableDirectBuffer;
import org.agrona.DirectBuffer;

@javax.annotation.Generated(value = {"onyx.serialization.MessageDecoder"})
@SuppressWarnings("all")
public class MessageDecoder
{
    public static final int BLOCK_LENGTH = 10;
    public static final int TEMPLATE_ID = 1;
    public static final int SCHEMA_ID = 1;
    public static final int SCHEMA_VERSION = 0;

    private final MessageDecoder parentMessage = this;
    private DirectBuffer buffer;
    protected int offset;
    protected int limit;
    protected int actingBlockLength;
    protected int actingVersion;

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

    public DirectBuffer buffer()
    {
        return buffer;
    }

    public int offset()
    {
        return offset;
    }

    public MessageDecoder wrap(
        final DirectBuffer buffer, final int offset, final int actingBlockLength, final int actingVersion)
    {
        this.buffer = buffer;
        this.offset = offset;
        this.actingBlockLength = actingBlockLength;
        this.actingVersion = actingVersion;
        limit(offset + actingBlockLength);

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

    public static int replicaVersionId()
    {
        return 2;
    }

    public static int replicaVersionSinceVersion()
    {
        return 0;
    }

    public static int replicaVersionEncodingOffset()
    {
        return 0;
    }

    public static int replicaVersionEncodingLength()
    {
        return 8;
    }

    public static String replicaVersionMetaAttribute(final MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case EPOCH: return "unix";
            case TIME_UNIT: return "nanosecond";
            case SEMANTIC_TYPE: return "";
        }

        return "";
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

    public long replicaVersion()
    {
        return buffer.getLong(offset + 0, java.nio.ByteOrder.LITTLE_ENDIAN);
    }


    public static int destIdId()
    {
        return 3;
    }

    public static int destIdSinceVersion()
    {
        return 0;
    }

    public static int destIdEncodingOffset()
    {
        return 8;
    }

    public static int destIdEncodingLength()
    {
        return 2;
    }

    public static String destIdMetaAttribute(final MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case EPOCH: return "unix";
            case TIME_UNIT: return "nanosecond";
            case SEMANTIC_TYPE: return "";
        }

        return "";
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

    public int destId()
    {
        return (buffer.getShort(offset + 8, java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF);
    }


    private final SegmentsDecoder segments = new SegmentsDecoder();

    public static long segmentsDecoderId()
    {
        return 4;
    }

    public static int segmentsDecoderSinceVersion()
    {
        return 0;
    }

    public SegmentsDecoder segments()
    {
        segments.wrap(parentMessage, buffer);
        return segments;
    }

    public static class SegmentsDecoder
        implements Iterable<SegmentsDecoder>, java.util.Iterator<SegmentsDecoder>
    {
        private static final int HEADER_SIZE = 4;
        private final GroupSizeEncodingDecoder dimensions = new GroupSizeEncodingDecoder();
        private MessageDecoder parentMessage;
        private DirectBuffer buffer;
        private int count;
        private int index;
        private int offset;
        private int blockLength;

        public void wrap(
            final MessageDecoder parentMessage, final DirectBuffer buffer)
        {
            this.parentMessage = parentMessage;
            this.buffer = buffer;
            dimensions.wrap(buffer, parentMessage.limit());
            blockLength = dimensions.blockLength();
            count = dimensions.numInGroup();
            index = -1;
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

        public int actingBlockLength()
        {
            return blockLength;
        }

        public int count()
        {
            return count;
        }

        public java.util.Iterator<SegmentsDecoder> iterator()
        {
            return this;
        }

        public void remove()
        {
            throw new UnsupportedOperationException();
        }

        public boolean hasNext()
        {
            return (index + 1) < count;
        }

        public SegmentsDecoder next()
        {
            if (index + 1 >= count)
            {
                throw new java.util.NoSuchElementException();
            }

            offset = parentMessage.limit();
            parentMessage.limit(offset + blockLength);
            ++index;

            return this;
        }

        public static int segmentBytesId()
        {
            return 5;
        }

        public static int segmentBytesSinceVersion()
        {
            return 0;
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

        public int segmentBytesLength()
        {
            final int limit = parentMessage.limit();
            return (int)(buffer.getInt(limit, java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF_FFFFL);
        }

        public int getSegmentBytes(final MutableDirectBuffer dst, final int dstOffset, final int length)
        {
            final int headerLength = 4;
            final int limit = parentMessage.limit();
            final int dataLength = (int)(buffer.getInt(limit, java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF_FFFFL);
            final int bytesCopied = Math.min(length, dataLength);
            parentMessage.limit(limit + headerLength + dataLength);
            buffer.getBytes(limit + headerLength, dst, dstOffset, bytesCopied);

            return bytesCopied;
        }

        public int getSegmentBytes(final byte[] dst, final int dstOffset, final int length)
        {
            final int headerLength = 4;
            final int limit = parentMessage.limit();
            final int dataLength = (int)(buffer.getInt(limit, java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF_FFFFL);
            final int bytesCopied = Math.min(length, dataLength);
            parentMessage.limit(limit + headerLength + dataLength);
            buffer.getBytes(limit + headerLength, dst, dstOffset, bytesCopied);

            return bytesCopied;
        }

        public String segmentBytes()
        {
            final int headerLength = 4;
            final int limit = parentMessage.limit();
            final int dataLength = (int)(buffer.getInt(limit, java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF_FFFFL);
            parentMessage.limit(limit + headerLength + dataLength);
            final byte[] tmp = new byte[dataLength];
            buffer.getBytes(limit + headerLength, tmp, 0, dataLength);

            final String value;
            try
            {
                value = new String(tmp, "UTF-8");
            }
            catch (final java.io.UnsupportedEncodingException ex)
            {
                throw new RuntimeException(ex);
            }

            return value;
        }


        public String toString()
        {
            return appendTo(new StringBuilder(100)).toString();
        }

        public StringBuilder appendTo(final StringBuilder builder)
        {
            builder.append('(');
            //Token{signal=BEGIN_VAR_DATA, name='segmentBytes', description='null', id=5, version=0, encodedLength=0, offset=0, componentTokenCount=6, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            builder.append("segmentBytes=");
            builder.append(segmentBytes());
            builder.append(')');
            return builder;
        }
    }


    public String toString()
    {
        return appendTo(new StringBuilder(100)).toString();
    }

    public StringBuilder appendTo(final StringBuilder builder)
    {
        final int originalLimit = limit();
        limit(offset + actingBlockLength);
        builder.append("[Message](sbeTemplateId=");
        builder.append(TEMPLATE_ID);
        builder.append("|sbeSchemaId=");
        builder.append(SCHEMA_ID);
        builder.append("|sbeSchemaVersion=");
        if (parentMessage.actingVersion != SCHEMA_VERSION)
        {
            builder.append(parentMessage.actingVersion);
            builder.append('/');
        }
        builder.append(SCHEMA_VERSION);
        builder.append("|sbeBlockLength=");
        if (actingBlockLength != BLOCK_LENGTH)
        {
            builder.append(actingBlockLength);
            builder.append('/');
        }
        builder.append(BLOCK_LENGTH);
        builder.append("):");
        //Token{signal=BEGIN_FIELD, name='replicaVersion', description='null', id=2, version=0, encodedLength=0, offset=0, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='uint64', description='null', id=-1, version=0, encodedLength=8, offset=0, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=UINT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.append("replicaVersion=");
        builder.append(replicaVersion());
        builder.append('|');
        //Token{signal=BEGIN_FIELD, name='destId', description='null', id=3, version=0, encodedLength=0, offset=8, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='uint16', description='null', id=-1, version=0, encodedLength=2, offset=8, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=UINT16, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.append("destId=");
        builder.append(destId());
        builder.append('|');
        //Token{signal=BEGIN_GROUP, name='segments', description='null', id=4, version=0, encodedLength=0, offset=10, componentTokenCount=12, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='null', timeUnit=null, semanticType='null'}}
        builder.append("segments=[");
        SegmentsDecoder segments = segments();
        if (segments.count() > 0)
        {
            while (segments.hasNext())
            {
                segments.next().appendTo(builder);
                builder.append(',');
            }
            builder.setLength(builder.length() - 1);
        }
        builder.append(']');

        limit(originalLimit);

        return builder;
    }
}
