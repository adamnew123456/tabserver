// -*- mode: csharp; fill-column: 100 -*-
using System.IO;
using System.Numerics;
using System.Text;

namespace brokerlib;

/// What type of data a frame contains or (for control frames) what action the frame requires.
public enum MessageType
{
	/// A frame in a series of fragmented frames, the first frame contains the real type.
	Continuation,

	/// Data frame containing UTF-8 encoded text
	Text,

	/// Data frame containing arbitrary data
	Binary,

	/// Control frame indicating the conneciton is dead
	Close,

	/// Control frame requesting a Pong response
	Ping,

	/// Control frame sent in response to a Ping
	Pong,
}

/// Constants and utility functions for implementing the WebSocket protocol.
public static class WebSocketProto
{
    public const int FIN_OFFSET = 7;
    public const int RSV_OFFSET = 4;
    public const int OPCODE_OFFSET = 0;

	public const byte FIN_FLAG = 0b10000000;
	public const byte RSV_FLAG = 0b01110000;
	public const byte OPCODE_FLAG = 0b00001111;

	public const int MASK_OFFSET = 7;
	public const int PAYLOAD_LEN_OFFSET = 0;

	public const byte MASK_FLAG = 0b10000000;
	public const byte PAYLOAD_LEN_FLAG = 0b01111111;

	public const byte PAYLOAD_64_BITS = 127;
	public const byte PAYLOAD_16_BITS = 126;

	public const byte OP_CONTINUATION = 0;
	public const byte OP_TEXT = 1;
	public const byte OP_BINARY = 2;
	public const byte OP_CLOSE = 8;
	public const byte OP_PING = 9;
	public const byte OP_PONG = 10;

	public const string HANDSHAKE_KEY = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

	/// Gets the MessageType for an opcode value.
	public static MessageType OpcodeToMessageType(int opcode)
	{
		switch (opcode)
		{
			case WebSocketProto.OP_CONTINUATION: return MessageType.Continuation;
			case WebSocketProto.OP_TEXT: return MessageType.Text;
			case WebSocketProto.OP_BINARY: return MessageType.Binary;
			case WebSocketProto.OP_CLOSE: return MessageType.Close;
			case WebSocketProto.OP_PING: return MessageType.Ping;
			case WebSocketProto.OP_PONG: return MessageType.Pong;
		}

		throw new ArgumentException($"Opcode {opcode:x} not recognized");
	}

	/// Gets the opcode for a MessageType value.
	public static int MessageTypeToOpcode(MessageType type)
	{
		switch (type)
		{
			case MessageType.Continuation: return WebSocketProto.OP_CONTINUATION;
			case MessageType.Text: return WebSocketProto.OP_TEXT;
			case MessageType.Binary: return WebSocketProto.OP_BINARY;
			case MessageType.Close: return WebSocketProto.OP_CLOSE;
			case MessageType.Ping: return WebSocketProto.OP_PING;
			case MessageType.Pong: return WebSocketProto.OP_PONG;
		}

		throw new ArgumentException($"MessageType {type} not recognized");
	}

	/// XORs each mask byte with each payload byte, in-place.
	public static void UnmaskFrame(Span<byte> frame, byte[] mask, int maskOffset=0)
	{
		// The naive way would look like this:
		//
		// for (var i = 0; i < frame.Length; i++)
		//   frame[i] = frame[i] ^ mask[(maskOffset + i) & 3]
		//
		// This is slow because it's performing the XOR 1 byte at a time, when the machine could do
		// them 4 (or more) bytes at a time at no extra cost. We can get a big speedup by treating
		// the mask as a single 32-bit integer and treating the frame as a collection of 32-bit
		// integers.
		//
		// This means we have to clean up the edges on both sides:
		//
		// - At the start, maskOffset may not be a multiple of 4. This isn't strictly required, but
		//   "aligning" the offets to the start of the mask simplifies converting the mask to an
		//   int. Without the alignment, we'd have to rotate the mask so that the first byte
		//   (depending on endianness!) corresponds to the initial maskOffset.
		//
		// - At the end, the remaining data in the frame may not be a multiple of 4.
		//
		// We can get an even bigger speedup by moving beyond simple types into SIMD types. A
		// machine that supports AVX2 supports 256-bit vector registers that can operate on 8 32-bit
		// integers in one go. This means that each individual XOR operates on 32x as much data as
		// the naive loop case.
		//
		// In terms of the numbers (recorded on a Ryzen 7 3700X), this is the time it takes to
		// unmask 1000 bytes worth of data:
		//
		//   Naive: 702 ns
		//   int32: 190 ns
		//   int64: 107 ns
		//   SIMD:   39 ns
		var maskInt = BitConverter.ToUInt32(mask);
		var maskIdx = maskOffset & 3;
		var frameOffset = 0;

		// Re-align the mask offset to the mask, so we can apply it directly without any shifting
		while (frameOffset < frame.Length)
		{
			if (maskIdx == 0 || maskIdx == 4) break;
			frame[frameOffset] = (byte)(frame[frameOffset] ^ mask[maskIdx]);
			frameOffset++;
			maskIdx++;
		}

		if (Vector.IsHardwareAccelerated)
		{
			var maskVec = new Vector<uint>(maskInt);
			var vecByteSize = Vector<uint>.Count * sizeof(uint);
			while (frame.Length - frameOffset >= vecByteSize)
			{
				var block = frame.Slice(frameOffset, vecByteSize);
				var blockVec = new Vector<uint>(block);
				blockVec ^= maskVec;
				blockVec.CopyTo(block);
				frameOffset += vecByteSize;
			}
		}

        while (frame.Length - frameOffset >= 4)
		{
			var block = frame.Slice(frameOffset, 4);
			var frameInt = BitConverter.ToUInt32(block);
			BitConverter.TryWriteBytes(block, frameInt ^ maskInt);
			frameOffset += 4;
		}

		// Clean up anything that doesn't fit within the blocks
		maskIdx = 0;
		while (frameOffset < frame.Length)
		{
			frame[frameOffset] = (byte)(frame[frameOffset] ^ mask[maskIdx]);
			frameOffset++;
			maskIdx++;
		}
	}

}

/// A parsed WebSocket message, possibly being sent over the wire as multiple frames. The message
/// may be used in one of two ways:
///
/// - When the WebSocketClientParser parses a frame and sends it to its callback, the message is
///   deallocated when the callback returns. This is to allow reusing the input buffer when parsing
///   messages.
///
/// - If the message needs to last beyond the callback, it should be copied to an owned version that
///   is responsible for managing its own buffer.
public class WebSocketMessage : IDisposable, IEquatable<WebSocketMessage>
{
	/// The type of message this frame contains. Never contains the Continuation value.
	public MessageType Type { get; private set; }

	/// A read-only view onto the frame's payload. Thorws if the frame's buffer has been disposed.
	public Span<byte> Payload {
		get {
			if (DataBuffer == null)
			{
				throw new InvalidOperationException("Cannot get payload outside of frame's scope.");
			}
			var buffer = DataBuffer.Value;
			return new Span<byte>(buffer.Array, buffer.Offset, buffer.Count);
		}
	}

	/// The actual storage for the frame's data.
	private ArraySegment<byte>? DataBuffer;

	/// Whether the data buffer is owned by the frame or not.
	private bool OwnsDataBuffer;

	/// Decodes the payload data into a string. Throws if the frame is not text.
	public string Text {
		get {
			if (Type != MessageType.Text)
			{
				throw new InvalidOperationException($"Cannot get Text from {Type} frame");
			}
			return Encoding.UTF8.GetString(Payload);
		}
	}

	/// Creates a WebSocketClientFrame that borrows its payload data from the given array segment.
	public WebSocketMessage(MessageType type, ArraySegment<byte> buffer)
	{
		Type = type;
		DataBuffer = buffer;
		OwnsDataBuffer = false;
	}

	/// Creates a WebSocketClientFrame that owns its payload data.
	public WebSocketMessage(MessageType type, byte[] buffer)
	{
		Type = type;
		DataBuffer = new ArraySegment<byte>(buffer);
		OwnsDataBuffer = true;
	}

	/// Creates a copy of this message that owns its own memory.
	public WebSocketMessage ToOwned()
	{
		if (DataBuffer == null)
		{
			throw new InvalidOperationException("Cannot clone payload outside of frame's scope.");
		}

		var buffer = new byte[DataBuffer.Value.Count];
		DataBuffer.Value.CopyTo(buffer);
		return new WebSocketMessage(Type, buffer);
	}

	public bool Equals(WebSocketMessage? other)
	{
		return other != null
			&& Type == other.Type
			&& Payload.SequenceEqual(other.Payload);
	}

	public override string ToString()
	{
		var head = Payload;
		var trailer = "";
		if (head.Length > 20)
		{
			trailer = $"... ({head.Length} bytes total)";
			head = head.Slice(0, 20);
		}

		var encoded = Convert.ToBase64String(head);
		return $"WebSocketClientFrame(Type={Type}, Payload={encoded} {trailer})";
	}

	public void Dispose()
	{
		if (DataBuffer != null && !OwnsDataBuffer)
		{
			DataBuffer = null;
		}
		GC.SuppressFinalize(this);
	}
}

/// Error indicating that the WebSocketClientParser encountered an invalid WebSocket frame. The
/// WebSocket connection must be closed when this exception is caught.
public class WebSocketParseException : Exception
{
	public WebSocketParseException(string reason): base(reason) {}
}

public sealed class WebSocketClientParser : IDisposable
{
	private const int FEED_BUFFER_SIZE = 8192;
	private static readonly byte[] EMPTY = new byte[0];

	// Each state indicates a differnt region of the frame that we could be expecting next. The
	// state is on a per-byte level, which means that adjacent bit flags do not need their own
	// states, but we also need extra data to track our current position inside of multi-byte units
	// and buffer them as necessary.
	//
    //  0                   1                   2                   3
    //  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    // +-+-+-+-+-------+-+-------------+-------------------------------+
    // |F|R|R|R| opcode|M| Payload len |    Extended payload length    |
    // |I|S|S|S|  (4)  |A|     (7)     |             (16/64)           |
    // |N|V|V|V|       |S|             |   (if payload len==126/127)   |
    // | |1|2|3|       |K|             |                               |
    // +-+-+-+-+-------+-+-------------+ - - - - - - - - - - - - - - - +
    // |     Extended payload length continued, if payload len == 127  |
    // + - - - - - - - - - - - - - - - +-------------------------------+
    // |                               |Masking-key, if MASK set to 1  |
    // +-------------------------------+-------------------------------+
    // | Masking-key (continued)       |          Payload Data         |
    // +-------------------------------- - - - - - - - - - - - - - - - +
    // :                     Payload Data continued ...                :
    // + - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - +
    // |                     Payload Data continued ...                |
    // +---------------------------------------------------------------+

	private enum ParserState
	{
		/// No packet data has been parsed yet, expecting the bit flags and opcode
		FlagsAndOpcode,

		/// Expecting the mask bit and the first part of the payload length
		MaskAndPayloadLength,

		/// Expecting the extended payload length. Unused if the frame has less than 126 bytes of
		/// data
		ExtendedPayloadLength,

		/// Expecting the masking key.
		MaskingKey,

		/// Expeceting payload data. The last state before the payload length is exhausted.
		Data,
	}

	/// The buffers that store previous data fragment bodies.
	private List<ArraySegment<byte>> DataBuffers;

	/// The buffers that store previous control fragment bodies. While control messages cannot be
	/// fragmented within the WebSocket protocol, they can come in over multiple TCP segments.
	private List<ArraySegment<byte>> ControlBuffers;

	/// The buffer that's being used by the current Feed operation.
	private byte[]? FeedBuffer;

	/// Where the previous Feed stopped processing data
	private ParserState State;

	/// Whether the last non-control frame was fragmented or not.
	private bool ExpectFragment;

	/// The size of all fragments received so far.
	private long CombinedFragmentSize;

	/// Whether the current frame is continuing some previous frame. True
	/// corresponds to a FIN bit of 0.
	private bool IsContinuation;

	/// Whether the current frame is a control message or not.
	private bool IsControl;

	/// The value of the Opcode field. Only set for the first frame in a sequence, and only used for
	/// data opcodes. See ControlOpcode for more information.
	private MessageType DataOpcode;

	/// The value of the Opcode field for control messages. This must be stored separately from the
	/// data opcode because control messages can be interleaved while a data message still has
	/// pending fragments.
	private MessageType ControlOpcode;

	/// A general counter whose use depends upon the current state:
	///
	/// - ExtendedPayloadLength: the number of payload length bytes remaining
	/// - MaskingKey: the number of masking key bytes remaining
	/// - Data: the number of data bytes remaining
	private int Counter;

	/// The size of the payload in bytes. Note that payloads longer than int32.MaxSize are not
	/// supported.
	private int PayloadSize;

	/// The payload mask.
	private byte[] Mask;

	public WebSocketClientParser()
	{
		DataBuffers = new List<ArraySegment<byte>>();
		ControlBuffers = new List<ArraySegment<byte>>();
		Mask = new byte[4];
		Reset();
	}

	/// Checks out a buffer from the parser's memory pool. This buffer is owned by the parser and
	/// should only be used to fill data for the next Feed operation. Once the parser is disposed
	/// the memory should no longer be used.
	public ArraySegment<byte> RentFeedBuffer()
	{
		if (FeedBuffer == null)
		{
			FeedBuffer = ArrayPool<byte>.Shared.Rent(FEED_BUFFER_SIZE);
		}
		return new ArraySegment<byte>(FeedBuffer);
	}

	/// Cleans up any buffers we're holding for partially parsed frames.
	public void Dispose()
	{
		Reset();
		GC.SuppressFinalize(this);
	}

	/// Remove any accumulated state for the portion of the packet that has already been parsed.
	public void Reset()
	{
		foreach (var buffer in DataBuffers)
		{
			ArrayPool<byte>.Shared.Return(buffer.Array);
		}
		DataBuffers.Clear();

		foreach (var buffer in ControlBuffers)
		{
			ArrayPool<byte>.Shared.Return(buffer.Array);
		}
		ControlBuffers.Clear();

		if (FeedBuffer != null)
		{
			ArrayPool<byte>.Shared.Return(FeedBuffer);
			FeedBuffer = null;
		}

        State = ParserState.FlagsAndOpcode;
		ExpectFragment = false;
		IsControl = false;
	}

	/// Combines all the buffers within one of the buffer lists into a single buffer. Once the list
	/// is combined, each entry in the buffer list is returned to the pool.
	private ArraySegment<byte> FlushBufferList(List<ArraySegment<byte>> bufferList)
	{
		var combinedSize = bufferList.Select(b => b.Count).Sum();
		var combinedBuffer = ArrayPool<byte>.Shared.Rent(combinedSize);
		var offset = 0;
		foreach (var buffer in bufferList)
		{
			var bufferSegment = new ArraySegment<byte>(combinedBuffer, offset, buffer.Count);
			buffer.CopyTo(bufferSegment);
			offset += buffer.Count;
			ArrayPool<byte>.Shared.Return(buffer.Array);
		}

		bufferList.Clear();
		return new ArraySegment<byte>(combinedBuffer, 0, offset);
	}

	/// Processes the data within the rented feed buffer and invokes the callback once per frame.
	/// This may happen any number of times depending upon how long the current frame is and whether
	/// it is a continuation with remaining fragments.
	///
	/// The frame given to the callback *cannot* be used outside of that callback. In order to
	/// acheive zero-copy, the frame temporarily takes ownership of the entire feed buffer and then
	/// releases it when the callback has finished. After that point the frame's data belongs to the
	/// parser and cannot be retrieved using the frame instance.
	///
	/// The data is assumed to start at offset 0 within the feed buffer.
	public void Feed(int size, Action<WebSocketMessage> callback)
	{
		if (FeedBuffer == null)
		{
			throw new InvalidOperationException("Cannot feed data when the feed buffer is uninitialized");
		}

		var buffer = new Span<byte>(FeedBuffer, 0, size);
		var offset = 0;
		while (offset < size)
		{
			WebSocketMessage? frame = ReadNextFrame(buffer, ref offset);
			if (frame != null)
			{
				using (frame)
				{
					callback(frame);
				}
			}
		}
	}

	/// Reads at most one frame from the feed buffer, and returns that frame (if there is one) along
	/// with the next read offset.
	private WebSocketMessage? ReadNextFrame(Span<byte> input, ref int offset)
	{
		while (offset < input.Length)
		{
			var inputRemaining = input.Length - offset;
			switch (State)
			{
				case ParserState.FlagsAndOpcode:
					ParseFlagsAndOpcode(input, ref offset);
					break;
				case ParserState.MaskAndPayloadLength:
					ParseMaskAndPayloadLength(input, ref offset);
					break;
				case ParserState.ExtendedPayloadLength:
					ParseExtendedPayloadLength(input, ref offset);
					break;
				case ParserState.MaskingKey:
				{
					var alreadyCopied = 4 - Counter;
					var toCopy = Math.Min(Counter, inputRemaining);

					var source = input.Slice(offset, toCopy);
					var destination = new Span<byte>(Mask, alreadyCopied, toCopy);
					source.CopyTo(destination);

					Counter -= source.Length;
					if (Counter == 0)
					{
						State = ParserState.Data;
						Counter = PayloadSize;
					}

					offset += source.Length;

					if (State == ParserState.Data && PayloadSize == 0)
					{
						// Special case - the loop would normally see that the buffer is
						// exhausted and terminate here, but we need to flush frame with the
						// empty payload
						goto case ParserState.Data;
					}
					break;
				}
				case ParserState.Data:
				{
					var frame = ParseData(input, ref offset);
					if (frame != null) return frame;
					break;
				}
			}
		}

		return null;
	}

	private void ParseFlagsAndOpcode(Span<byte> input, ref int offset)
	{
		var flagsAndOpcode = input[offset];
		var fin = (flagsAndOpcode & WebSocketProto.FIN_FLAG) >> WebSocketProto.FIN_OFFSET;
		var rsv = (flagsAndOpcode & WebSocketProto.RSV_FLAG) >> WebSocketProto.RSV_OFFSET;
		var opcode = (flagsAndOpcode & WebSocketProto.OPCODE_FLAG) >> WebSocketProto.OPCODE_OFFSET;

		if (rsv != 0)
		{
			throw new WebSocketParseException($"Client set unsupported reserved flags: {flagsAndOpcode:x}/{rsv:x}");
		}

		// The RFC allows for control frames to be interleaved within a series of
		// fragmented message frames, but each of those control frames must be
		// self-contained. We're safe to leave the continuation status and message
		// buffers the same as they were when the last data fragment came in.
		if (opcode == WebSocketProto.OP_CLOSE
			|| opcode == WebSocketProto.OP_PING
			|| opcode == WebSocketProto.OP_PONG)
		{
			if (fin != 1)
			{
				throw new WebSocketParseException("Client sent a fragmented control frame");
			}

			IsControl = true;
			ControlOpcode = WebSocketProto.OpcodeToMessageType(opcode);
		}
		else if (opcode == WebSocketProto.OP_CONTINUATION)
		{
			if (!ExpectFragment)
			{
				throw new WebSocketParseException("Client sent a continuation but the previous frame was not fragmented");
			}

			ExpectFragment = fin == 0;
			IsContinuation = true;
			IsControl = false;
		}
		else
		{
			if (ExpectFragment)
			{
				throw new WebSocketParseException("Client sent a data frame but the previous fragment is not complete");
			}

			ExpectFragment = fin == 0;
			IsContinuation = false;
			IsControl = false;
			CombinedFragmentSize = 0;
			DataOpcode = WebSocketProto.OpcodeToMessageType(opcode);
		}

		State = ParserState.MaskAndPayloadLength;
		offset++;
	}

	private void ParseMaskAndPayloadLength(Span<byte> input, ref int offset)
	{
		var maskAndLength = input[offset];
		var mask = (maskAndLength & WebSocketProto.MASK_FLAG) >> WebSocketProto.MASK_OFFSET;
		var length = (maskAndLength & WebSocketProto.PAYLOAD_LEN_FLAG) >> WebSocketProto.PAYLOAD_LEN_OFFSET;

		if (mask != 1)
		{
			throw new WebSocketParseException("Client sent unmasked frame");
		}

		if (length == WebSocketProto.PAYLOAD_16_BITS)
		{
			State = ParserState.ExtendedPayloadLength;
			PayloadSize = 0;
			Counter = 2;
		}
		else if (length == WebSocketProto.PAYLOAD_64_BITS)
		{
			State = ParserState.ExtendedPayloadLength;
			PayloadSize = 0;
			Counter = 8;
		}
		else
		{
			State = ParserState.MaskingKey;
			Counter = 4;
			PayloadSize = length;
		}

		offset++;
	}

	private void ParseExtendedPayloadLength(Span<byte> input, ref int offset)
	{
		PayloadSize <<= 8;
		PayloadSize |= input[offset];

		if (Counter == 4)
		{
			if (PayloadSize != 0)
			{
				// The end of the upper 32-bits of the length. Any data there indicates that our size
				// limit is being exceeded.
				throw new WebSocketParseException($"Frame payload size {PayloadSize} is over 2 GiB");
			}

			PayloadSize = 0;
		}

		// We're lenient here about whether the payload length is the most compact
		// representation. The RFC says that a value below 126 must use the 7-bit
		// encoding and a value below 65536 must use the 16-bit encoding, but checking
		// that would require storing the original 7-bit length or doing some kind of
		// analysis to find the maximum number of leading 0 bytes. Both of these add
		// extra work that doesn't help us avoid any invalid states.

		if (Counter == 1)
		{
			State = ParserState.MaskingKey;
			Counter = 4;
		}
		else
		{
			Counter--;
		}

		offset++;
	}

	private WebSocketMessage? ParseData(Span<byte> input, ref int offset)
	{
		var inputRemaining = input.Length - offset;
		var toCopy = Math.Min(inputRemaining, Counter);
		var targetBufferList = IsControl ? ControlBuffers : DataBuffers;

		var type = IsControl ? ControlOpcode : DataOpcode;
		var isSelfContained = (IsControl || (!IsContinuation && !ExpectFragment))
			&& toCopy == Counter
			&& targetBufferList.Count == 0;

		if (isSelfContained)
		{
			var containedBuffer = new ArraySegment<byte>(FeedBuffer, offset, toCopy);
			WebSocketProto.UnmaskFrame(containedBuffer, Mask);
			State = ParserState.FlagsAndOpcode;
			Counter -= toCopy;
			offset += toCopy;
			return new WebSocketMessage(type, containedBuffer);
		}

		if (toCopy > 0)
		{
			if (!IsControl)
			{
				var newCombinedSize = CombinedFragmentSize + toCopy;
				if (newCombinedSize > int.MaxValue)
				{
					throw new WebSocketParseException($"Combined fragments have size {CombinedFragmentSize} which is over 2 GiB");
				}
				CombinedFragmentSize = newCombinedSize;
			}

			var buffer = ArrayPool<byte>.Shared.Rent(toCopy);
			var payload = new Span<byte>(FeedBuffer, offset, toCopy);
			payload.CopyTo(buffer);

			var framePosition = PayloadSize - Counter;
			var payloadSegment = new ArraySegment<byte>(buffer, 0, toCopy);
			WebSocketProto.UnmaskFrame(payloadSegment, Mask, framePosition);
			targetBufferList.Add(payloadSegment);
		}

		Counter -= toCopy;
		offset += toCopy;
		if (Counter > 0) return null;

		State = ParserState.FlagsAndOpcode;
		if (!IsControl && ExpectFragment) return null;

		var combinedBuffer = FlushBufferList(targetBufferList);
		return new WebSocketMessage(type, combinedBuffer);
	}
}

public class WebSocketFrame
{
	/// The FIN flag, indicates that this is the last fragment of a message. Always true for control
	/// messages.
	public bool IsLastFragment { get; set; }

	/// The type of data this frame contains.
	public MessageType OpCode { get; set; }

	/// The byte array used for masking. If not set, no masking is performed and the MASK flag is
	/// zero.
	public byte[]? Mask {
		get { return _Mask; }
		set {
			if (value != null && value.Length != 4)
			{
				throw new ArgumentException($"WebSocket frame mask must be 4 bytes");
			}
			_Mask = value;
		}
	}

	private byte[]? _Mask;

	/// The total size of the payload.
	public int Payload { get; set; }

	/// Determines the size of the buffer required to serialized this frame, including both its
	/// header and its data.
	public int RequiredCapacity()
	{
		// Including flags/opcode and base payload length
		var size = 2;
		if (Mask != null)
		{
			size += 4;
		}

		if (Payload > UInt16.MaxValue)
		{
			size += 8;
		}
		else if (Payload > 125)
		{
			size += 2;
		}
		size += Payload;

		return size;
	}

	/// Serializes the header of the frame to the given buffer. Returns the number of bytes written.
	private int WriteHeader(Span<byte> destination)
	{
		var offset = 0;
		var flagsOpCode = (IsLastFragment ? 1 : 0) << WebSocketProto.FIN_OFFSET
			| WebSocketProto.MessageTypeToOpcode(OpCode);
		destination[offset++] = (byte) flagsOpCode;

		var maskBit = (Mask == null ? 0 : 1) << WebSocketProto.MASK_OFFSET;
		if (Payload <= 125)
		{
			destination[offset++] = (byte) (maskBit | Payload);
		}
		else if (Payload <= UInt16.MaxValue)
		{
			destination[offset++] = (byte)(maskBit | 126);
			destination[offset++] = (byte)(Payload >> 8);
			destination[offset++] = (byte)Payload;
		}
		else
		{
			destination[offset++] = (byte)(maskBit | 127);

			// byte[].Length fits in an int, the upper 32-bits are not needed to represent the
			// length
			destination[offset++] = 0;
			destination[offset++] = 0;
			destination[offset++] = 0;
			destination[offset++] = 0;
			destination[offset++] = (byte)(Payload >> 24);
			destination[offset++] = (byte)(Payload >> 16);
			destination[offset++] = (byte)(Payload >> 8);
			destination[offset++] = (byte)Payload;
		}

		if (Mask != null)
		{
			destination[offset++] = Mask[0];
			destination[offset++] = Mask[1];
			destination[offset++] = Mask[2];
			destination[offset++] = Mask[3];
		}

		return offset;
	}

	/// Serializes the frame header to the given position within the byte array. Returns a Span
	/// designating the space where the payload data must be copied.
	public Span<byte> WriteHeaderTo(Span<byte> destination)
	{
		var headerSize = WriteHeader(destination);
		return destination.Slice(headerSize, Payload);
	}

	/// Masks the payload data within the given span, obtianed from WriteHeaderTo.
	public void MaskPayloadInBuffer(Span<byte> payload)
	{
		if (Mask == null) return;
		WebSocketProto.UnmaskFrame(payload, Mask);
	}

	/// Serializes the frame to a new byte array.
	public byte[] ToArray(byte[]? payload)
	{
		if ((payload == null && Payload != 0)
			|| (payload != null && Payload != payload.Length))
		{
			throw new ArgumentException("Provided data buffer has different size than Payload field.");
		}

		var buffer = new byte[RequiredCapacity()];
		var dataSpan = WriteHeaderTo(buffer);
		if (payload != null)
		{
			new Span<byte>(payload).CopyTo(dataSpan);
			MaskPayloadInBuffer(dataSpan);
		}
		return buffer;
	}

	public override string ToString()
	{
		return $"Frame<IsLastFragment={IsLastFragment}, OpCode={OpCode}, Mask={Mask}, Payload={Payload}>";
	}
}
