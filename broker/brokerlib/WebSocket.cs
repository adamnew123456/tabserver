// -*- mode: csharp; fill-column: 100 -*-
using System.IO;
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

/// A parsed WebSocket frame. This does not preserve any information about fragmentation - each
/// client frame is a complete message, with the content being all fragments content combined and in
/// order.
public class WebSocketClientFrame : IDisposable, IEquatable<WebSocketClientFrame>
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
	public WebSocketClientFrame(MessageType type, ArraySegment<byte> buffer)
	{
		Type = type;
		DataBuffer = buffer;
		OwnsDataBuffer = false;
	}

	/// Creates a WebSocketClientFrame that owns its payload data.
	public WebSocketClientFrame(MessageType type, byte[] buffer)
	{
		Type = type;
		DataBuffer = new ArraySegment<byte>(buffer);
		OwnsDataBuffer = true;
	}

	public WebSocketClientFrame ToOwned()
	{
		if (DataBuffer == null)
		{
			throw new InvalidOperationException("Cannot clone payload outside of frame's scope.");
		}

		var buffer = new byte[DataBuffer.Value.Count];
		DataBuffer.Value.CopyTo(buffer);
		return new WebSocketClientFrame(Type, buffer);
	}

	public bool Equals(WebSocketClientFrame? other)
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
	public static void UnmaskFrame(ArraySegment<byte> frame, byte[] mask, int maskOffset=0)
	{
		var maskIdx = maskOffset % 4;
		for (var i = 0; i < frame.Count; i++)
		{
			frame[i] = (byte)(frame[i] ^ mask[maskIdx % 4]);
			maskIdx++;
		}
	}

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

	/// The pool that owns the buffers where we store response bodies that span
	/// multiple Feed calls.
	private ArrayPool<byte> BufferPool;

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
		BufferPool = ArrayPool<byte>.Shared;
		DataBuffers = new List<ArraySegment<byte>>();
		ControlBuffers = new List<ArraySegment<byte>>();
		Mask = new byte[4];
		Reset();
	}

	/// Checks out a buffer from the parser's memory pool. This buffer is owned by the parser and
	/// should only be used to fill data for the next Feed operation. Once the parser is disposed
	/// the memory should no longer be used.
	public Memory<byte> RentFeedBuffer()
	{
		if (FeedBuffer == null)
		{
			FeedBuffer = BufferPool.Rent(FEED_BUFFER_SIZE);
		}
		return new Memory<byte>(FeedBuffer);
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
			BufferPool.Return(buffer.Array);
		}
		DataBuffers.Clear();

		foreach (var buffer in ControlBuffers)
		{
			BufferPool.Return(buffer.Array);
		}
		ControlBuffers.Clear();

		if (FeedBuffer != null)
		{
			BufferPool.Return(FeedBuffer);
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
		var combinedBuffer = BufferPool.Rent(combinedSize);
		var offset = 0;
		foreach (var buffer in bufferList)
		{
			var bufferSegment = new ArraySegment<byte>(combinedBuffer, offset, buffer.Count);
			buffer.CopyTo(bufferSegment);
			offset += buffer.Count;
			BufferPool.Return(buffer.Array);
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
	public void Feed(int size, Action<WebSocketClientFrame> callback)
	{
		if (FeedBuffer == null)
		{
			throw new InvalidOperationException("Cannot feed data when the feed buffer is uninitialized");
		}

		var buffer = new Span<byte>(FeedBuffer, 0, size);
		var offset = 0;
		while (offset < size)
		{
			WebSocketClientFrame? frame = ReadNextFrame(buffer, ref offset);
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
	private WebSocketClientFrame? ReadNextFrame(Span<byte> input, ref int offset)
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

	private WebSocketClientFrame? ParseData(Span<byte> input, ref int offset)
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
			return new WebSocketClientFrame(type, containedBuffer);
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

			var buffer = BufferPool.Rent(toCopy);
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
		return new WebSocketClientFrame(type, combinedBuffer);
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

	/// The payload. If the MessageType is text, this must be valid UTF-8 after combining with the
	/// other fragments.
	public byte[]? Payload { get; set; }

	/// Serializes the frame to the given output stream
	public void Write(Stream output)
	{
		var flagsOpcode = (IsLastFragment ? 1 : 0) << WebSocketProto.FIN_OFFSET
			| WebSocketProto.MessageTypeToOpcode(OpCode);
		output.WriteByte((byte) flagsOpcode);

		var maskBit = (Mask == null ? 0 : 1) << WebSocketProto.MASK_OFFSET;
		var payloadSize = Payload?.Length ?? 0;
		if (payloadSize <= 125)
		{
			output.WriteByte((byte)(maskBit | payloadSize));
		}
		else if (payloadSize <= UInt16.MaxValue)
		{
			output.WriteByte((byte)(maskBit | 126));
			output.WriteByte((byte)(payloadSize >> 8));
			output.WriteByte((byte)payloadSize);
		}
		else
		{
			output.WriteByte((byte)(maskBit | 127));

			// byte[].Length fits in an int, the upper 32-bits are not needed to represent the
			// length
			output.WriteByte(0);
			output.WriteByte(0);
			output.WriteByte(0);
			output.WriteByte(0);
			output.WriteByte((byte)(payloadSize >> 24));
			output.WriteByte((byte)(payloadSize >> 16));
			output.WriteByte((byte)(payloadSize >> 8));
			output.WriteByte((byte)payloadSize);
		}

		if (Mask != null)
		{
			output.Write(Mask);
		}

		if (Payload != null)
		{
			if (Mask == null)
			{
				output.Write(Payload);
			}
			else
			{
				for (var i = 0; i < Payload.Length; i++)
				{
					output.WriteByte((byte)(Payload[i] ^ Mask[i % 4]));
				}
			}
		}
	}

	/// Serializes the frame to a new MemoryStream and returns its array
	public byte[] ToArray()
	{
		var buffer = new MemoryStream();
		Write(buffer);
		return buffer.ToArray();
	}
}
