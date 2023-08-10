// -*- mode: csharp; fill-column: 100 -*-
namespace brokerlib;

/// A wrapper around a basic buffer that provides operations that are useful for
/// socket receivers.
public struct ReceiveBuffer : IDisposable
{
    /// Backing storage for saved and newly written data
    private byte[] Buffer;

    /// The position in the buffer past the read data
    private int WriteCursor;

    public ReceiveBuffer(int size)
    {
        Buffer = ArrayPool<byte>.Shared.Rent(size);
        WriteCursor = 0;
    }

    public void Dispose()
    {
        ArrayPool<byte>.Shared.Return(Buffer);
    }

    /// Whether the buffer is currently full or not. If true, calling WritableSlice would return a
    /// 0-byte slice.
    public bool IsFull { get { return WriteCursor == Buffer.Length; } }

    /// Gets the slice that contains all readable data, including saved data
    /// from the last SaveUnread as well as any newly written data.
    public Memory<byte> ReadableSlice(int bytesWritten)
    {
        WriteCursor += bytesWritten;
        return new Memory<byte>(Buffer, 0, WriteCursor);
    }

    /// Gets the slice that contains the unwritten portion of the buffer.
    public Memory<byte> WritableSlice()
    {
        return new Memory<byte>(Buffer, WriteCursor, Buffer.Length - WriteCursor);
    }

    /// Copies any unread data to the start of the buffer and marks the rest of
    /// the buffer as unwritten.
    public void SaveUnread(int readBytes)
    {
        if (readBytes == WriteCursor)
        {
            // All available data has been written, no need to copy anything
            WriteCursor = 0;
            return;
        }
        else if (readBytes == 0)
        {
            // No progress was made on the current buffer
            return;
        }

        var unreadBytes = WriteCursor - readBytes;
        var unreadSlice = new Memory<byte>(Buffer, readBytes, unreadBytes);
        WriteCursor = 0;
        unreadSlice.CopyTo(WritableSlice());
        WriteCursor = unreadBytes;
    }
}
