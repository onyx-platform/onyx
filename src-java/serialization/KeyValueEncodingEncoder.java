/* Generated SBE (Simple Binary Encoding) message codec */
package onyx.serialization;

import org.agrona.MutableDirectBuffer;

@javax.annotation.Generated(value = {"onyx.serialization.KeyValueEncodingEncoder"})
@SuppressWarnings("all")
public class KeyValueEncodingEncoder
{
    public static final int ENCODED_LENGTH = -1;
    private int offset;
    private MutableDirectBuffer buffer;

    public KeyValueEncodingEncoder wrap(final MutableDirectBuffer buffer, final int offset)
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
        return 2;
    }

    public static int lengthNullValue()
    {
        return 65535;
    }

    public static int lengthMinValue()
    {
        return 0;
    }

    public static int lengthMaxValue()
    {
        return 65534;
    }

    public KeyValueEncodingEncoder length(final int value)
    {
        buffer.putShort(offset + 0, (short)value, java.nio.ByteOrder.LITTLE_ENDIAN);
        return this;
    }


    public static int varDataEncodingOffset()
    {
        return 2;
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
        KeyValueEncodingDecoder writer = new KeyValueEncodingDecoder();
        writer.wrap(buffer, offset);

        return writer.appendTo(builder);
    }
}
