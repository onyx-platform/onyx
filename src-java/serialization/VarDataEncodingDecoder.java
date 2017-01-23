/* Generated SBE (Simple Binary Encoding) message codec */
package onyx.serialization;

import org.agrona.DirectBuffer;

@javax.annotation.Generated(value = {"onyx.serialization.VarDataEncodingDecoder"})
@SuppressWarnings("all")
public class VarDataEncodingDecoder
{
    public static final int ENCODED_LENGTH = -1;
    private int offset;
    private DirectBuffer buffer;

    public VarDataEncodingDecoder wrap(final DirectBuffer buffer, final int offset)
    {
        this.buffer = buffer;
        this.offset = offset;

        return this;
    }

    public DirectBuffer buffer()
    {
        return buffer;
    }

    public int offset()
    {
        return offset;
    }

    public int encodedLength()
    {
        return ENCODED_LENGTH;
    }

    public static int lengthEncodingOffset()
    {
        return 0;
    }

    public static int lengthEncodingLength()
    {
        return 4;
    }

    public static long lengthNullValue()
    {
        return 4294967294L;
    }

    public static long lengthMinValue()
    {
        return 0L;
    }

    public static long lengthMaxValue()
    {
        return 1073741824L;
    }

    public long length()
    {
        return (buffer.getInt(offset + 0, java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF_FFFFL);
    }


    public static int varDataEncodingOffset()
    {
        return 4;
    }

    public static int varDataEncodingLength()
    {
        return -1;
    }

    public static byte varDataNullValue()
    {
        return (byte)-128;
    }

    public static byte varDataMinValue()
    {
        return (byte)-127;
    }

    public static byte varDataMaxValue()
    {
        return (byte)127;
    }

    public String toString()
    {
        return appendTo(new StringBuilder(100)).toString();
    }

    public StringBuilder appendTo(final StringBuilder builder)
    {
        builder.append('(');
        //Token{signal=ENCODING, name='length', description='null', id=-1, version=0, encodedLength=4, offset=0, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=UINT32, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=1073741824, nullValue=null, constValue=null, characterEncoding='UTF-8', epoch='null', timeUnit=null, semanticType='null'}}
        builder.append("length=");
        builder.append(length());
        builder.append('|');
        //Token{signal=ENCODING, name='varData', description='null', id=-1, version=0, encodedLength=-1, offset=4, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT8, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='UTF-8', epoch='null', timeUnit=null, semanticType='null'}}
        builder.append(')');

        return builder;
    }
}
