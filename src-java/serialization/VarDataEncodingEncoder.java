/* Generated SBE (Simple Binary Encoding) message codec */
package onyx.serialization;

import org.agrona.MutableDirectBuffer;

@javax.annotation.Generated(value = {"onyx.serialization.VarDataEncodingEncoder"})
@SuppressWarnings("all")
public class VarDataEncodingEncoder
{
    public static final int ENCODED_LENGTH = -1;
    private int offset;
    private MutableDirectBuffer buffer;

    public VarDataEncodingEncoder wrap(final MutableDirectBuffer buffer, final int offset)
    {
        this.buffer = buffer;
        this.offset = offset;

        return this;
    }

    public MutableDirectBuffer buffer()
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

    public VarDataEncodingEncoder length(final long value)
    {
        buffer.putInt(offset + 0, (int)value, java.nio.ByteOrder.LITTLE_ENDIAN);
        return this;
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
        VarDataEncodingDecoder writer = new VarDataEncodingDecoder();
        writer.wrap(buffer, offset);

        return writer.appendTo(builder);
    }
}
