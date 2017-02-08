/* Generated SBE (Simple Binary Encoding) message codec */
package onyx.serialization;

import org.agrona.MutableDirectBuffer;
import org.agrona.DirectBuffer;

@javax.annotation.Generated(value = {"onyx.serialization.BarrierDecoder"})
@SuppressWarnings("all")
public class BarrierDecoder
{
    public static final int BLOCK_LENGTH = 18;
    public static final int TEMPLATE_ID = 15;
    public static final int SCHEMA_ID = 1;
    public static final int SCHEMA_VERSION = 0;

    private final BarrierDecoder parentMessage = this;
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

    public BarrierDecoder wrap(
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
        return 16;
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


    public static int epochId()
    {
        return 17;
    }

    public static int epochSinceVersion()
    {
        return 0;
    }

    public static int epochEncodingOffset()
    {
        return 8;
    }

    public static int epochEncodingLength()
    {
        return 8;
    }

    public static String epochMetaAttribute(final MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case EPOCH: return "unix";
            case TIME_UNIT: return "nanosecond";
            case SEMANTIC_TYPE: return "";
        }

        return "";
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

    public long epoch()
    {
        return buffer.getLong(offset + 8, java.nio.ByteOrder.LITTLE_ENDIAN);
    }


    public static int destIdId()
    {
        return 18;
    }

    public static int destIdSinceVersion()
    {
        return 0;
    }

    public static int destIdEncodingOffset()
    {
        return 16;
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
        return (buffer.getShort(offset + 16, java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF);
    }


    public static int payloadBytesId()
    {
        return 19;
    }

    public static int payloadBytesSinceVersion()
    {
        return 0;
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

    public int payloadBytesLength()
    {
        final int limit = parentMessage.limit();
        return (int)(buffer.getInt(limit, java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF_FFFFL);
    }

    public int getPayloadBytes(final MutableDirectBuffer dst, final int dstOffset, final int length)
    {
        final int headerLength = 4;
        final int limit = parentMessage.limit();
        final int dataLength = (int)(buffer.getInt(limit, java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF_FFFFL);
        final int bytesCopied = Math.min(length, dataLength);
        parentMessage.limit(limit + headerLength + dataLength);
        buffer.getBytes(limit + headerLength, dst, dstOffset, bytesCopied);

        return bytesCopied;
    }

    public int getPayloadBytes(final byte[] dst, final int dstOffset, final int length)
    {
        final int headerLength = 4;
        final int limit = parentMessage.limit();
        final int dataLength = (int)(buffer.getInt(limit, java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF_FFFFL);
        final int bytesCopied = Math.min(length, dataLength);
        parentMessage.limit(limit + headerLength + dataLength);
        buffer.getBytes(limit + headerLength, dst, dstOffset, bytesCopied);

        return bytesCopied;
    }

    public String payloadBytes()
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
        final int originalLimit = limit();
        limit(offset + actingBlockLength);
        builder.append("[Barrier](sbeTemplateId=");
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
        //Token{signal=BEGIN_FIELD, name='replicaVersion', description='null', id=16, version=0, encodedLength=0, offset=0, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='uint64', description='null', id=-1, version=0, encodedLength=8, offset=0, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=UINT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.append("replicaVersion=");
        builder.append(replicaVersion());
        builder.append('|');
        //Token{signal=BEGIN_FIELD, name='epoch', description='null', id=17, version=0, encodedLength=0, offset=8, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='uint64', description='null', id=-1, version=0, encodedLength=8, offset=8, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=UINT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.append("epoch=");
        builder.append(epoch());
        builder.append('|');
        //Token{signal=BEGIN_FIELD, name='destId', description='null', id=18, version=0, encodedLength=0, offset=16, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='uint16', description='null', id=-1, version=0, encodedLength=2, offset=16, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=UINT16, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.append("destId=");
        builder.append(destId());
        builder.append('|');
        //Token{signal=BEGIN_VAR_DATA, name='payloadBytes', description='null', id=19, version=0, encodedLength=0, offset=18, componentTokenCount=6, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.append("payloadBytes=");
        builder.append(payloadBytes());

        limit(originalLimit);

        return builder;
    }
}
