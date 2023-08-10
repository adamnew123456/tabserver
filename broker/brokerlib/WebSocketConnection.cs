// -*- mode: csharp; fill-column: 100 -*-
using System.Security.Cryptography;
using System.IO;
using System.Text;
using System.Text.Json;

namespace brokerlib;

public enum WebSocketHandshakeStatus
{
	/// The handshake hasn't received the client's full request yet
	Pending,

	/// The handshake request was valid and the success response has been sent
	Successful,

	/// The handshake request was invalid and the failure response has been sent
	Failed,
}

/// Handles the initial handshake of the WebSocket session. Waits for an HTTP/1.1 GET, validates the
/// required headers, and then transitions to the WebSocketMessageState.
public class WebSocketHandshake : IManagedSocket
{
	/// HTTP status codes for errors that occur when processing the request
	private const int INVALID_REQUEST = 400;
	private const int NOT_FOUND = 404;
	private const int METHOD_NOT_ALLOWED = 405;
	private const int NOT_IMPLEMENTED = 501;

	/// The manager that executes our async socket requests.
	public ISocketManager Manager { get; private set; }

	/// Buffer used for storing data from the socket
	private ReceiveBuffer ReceiveBuffer = new ReceiveBuffer(4096);

	/// Whether the request line has been read from the request.
	private bool SeenRequestLine;

	/// Whether the Host header has been read from the request.
	private bool SeenHostHeader;

	/// Whether the Upgrade header has been read from the request.
	private bool SeenUpgradeHeader;

	/// Whether the Connection header has been read from the request.
	private bool SeenConnectionHeader;

	/// The value of the Sec-WebSocket-Key header. Null if the header has not been parsed.
	private string? WSKey;

	/// Whether the Sec-WebSocket-Version header has been read from the request.
	private bool SeenWSVersionHeader;

	/// Whether the handshake has been accepted or not. This value has no meaning until a call to
	/// ReadNextLine returns true.
	public WebSocketHandshakeStatus Status {get; private set; }

	public WebSocketHandshake(ISocketManager manager)
	{
		Manager = manager;
		Status = WebSocketHandshakeStatus.Pending;
	}

	public void OnConnected()
	{
		Manager.Receive(this, ReceiveBuffer.WritableSlice());
	}

	public void OnReceive(Memory<byte> destination)
	{
		var foundReturn = false;
		var lineStart = 0;
		var buffer = ReceiveBuffer.ReadableSlice(destination.Length).Span;

		for (var i = 0; i < buffer.Length; i++)
		{
			if (buffer[i] == '\r')
			{
				foundReturn = true;
			}
			else
			{
				if (foundReturn && buffer[i] == '\n')
				{
					var lineLength = (i - 1) - lineStart;
					var line = buffer.Slice(lineStart, lineLength);

					var done = ProcessLine(line);
					if (done) return;

					lineStart = i + 1;
				}

				foundReturn = false;
			}
		}

		// Abort if the line is too long. HTTP/1.1 has no strict line limit, but we don't expect any
		// requests that contain data that's too big to fit in the buffer.
		if (lineStart == 0 && ReceiveBuffer.IsFull)
		{
			Status = WebSocketHandshakeStatus.Failed;
			SendResponse(INVALID_REQUEST, "Line Too Long");
			return;
		}

		ReceiveBuffer.SaveUnread(lineStart);
		Manager.Receive(this, ReceiveBuffer.WritableSlice());
	}

	public void OnSend()
	{
	}

	public void OnClose()
	{
	}

	/// Constructs a response value to send to the client, once the request has been completely
	/// processed. The status code and details are only used when sending error responses.
	private void SendResponse(int status = 0, string? details = null)
	{
		string message;
		if (Status == WebSocketHandshakeStatus.Failed)
		{
			message = $"HTTP/1.1 {status} {details ?? "Unknown"}\r\n";
		}
		else
		{
			var hashedKey = Convert.ToBase64String(SHA1.HashData(Encoding.UTF8.GetBytes($"{WSKey}{WebSocketProto.HANDSHAKE_KEY}")));
			message = $"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {hashedKey}\r\n\r\n";
		}

		var data = Encoding.UTF8.GetBytes(message);
		Manager.SendAll(this, new Memory<byte>(data));
	}

	/// Processes a single line from the socket and acts on it. Returns true if the request is done
	/// parsing and false otherwise.
	private bool ProcessLine(Span<byte> line)
	{
		if (!SeenRequestLine)
		{
			SeenRequestLine = true;
			var lineString = Encoding.ASCII.GetString(line);
			var parts = lineString.Split(' ');
			if (parts.Length != 3 || parts[2] != "HTTP/1.1")
			{
				Status = WebSocketHandshakeStatus.Failed;
				SendResponse(INVALID_REQUEST, "Not HTTP11 Request Line");
				return true;
			}

			if (parts[0] != "GET")
			{
				Status = WebSocketHandshakeStatus.Failed;
				SendResponse(METHOD_NOT_ALLOWED, "Only GET Allowed");
				return true;
			}

			if (parts[1] != "/")
			{
				Status = WebSocketHandshakeStatus.Failed;
				SendResponse(NOT_FOUND, "Only Root Path Allowed");
				return true;
			}

			return false;
		}
		else
		{
			if (line.Length == 0)
			{
				if (!SeenHostHeader || !SeenUpgradeHeader || !SeenConnectionHeader || WSKey == null || !SeenWSVersionHeader)
				{
					Status = WebSocketHandshakeStatus.Failed;
					SendResponse(INVALID_REQUEST, "Missing Header");
				}
				else
				{
					Status = WebSocketHandshakeStatus.Successful;
					SendResponse();
				}

				return true;
			}
			else if (line[0] == ' ' || line[0] == '\t')
			{
				Status = WebSocketHandshakeStatus.Failed;
				SendResponse(NOT_IMPLEMENTED, "Unsupported Header Continuation");
				return true;
			}
			else
			{
				var lineString = Encoding.ASCII.GetString(line);
				int colonIndex = lineString.IndexOf(':');
				if (colonIndex == -1)
				{
					Status = WebSocketHandshakeStatus.Failed;
					SendResponse(INVALID_REQUEST, "Header Separator Not Found");
					return true;
				}

				var header = lineString.Substring(0, colonIndex).ToLowerInvariant();
				if (header == "host" && !SeenHostHeader)
				{
					SeenHostHeader = true;
					// Technically this has to be a hostname or IP address followed by an optional
					// port, but we don't care since we don't use this information.
				}
				else if (header == "upgrade" && !SeenUpgradeHeader)
				{
					SeenUpgradeHeader = true;
					var value = lineString.Substring(colonIndex + 1).Trim();
					if (!OptionInHeaderList(value, "websocket"))
					{
						Status = WebSocketHandshakeStatus.Failed;
						SendResponse(INVALID_REQUEST, "Upgrade must contain websocket");
						return true;
					}
				}
				else if (header == "connection" && !SeenConnectionHeader)
				{
					SeenConnectionHeader = true;
					var value = lineString.Substring(colonIndex + 1).Trim();
					if (!OptionInHeaderList(value, "upgrade"))
					{
						Status = WebSocketHandshakeStatus.Failed;
						SendResponse(INVALID_REQUEST, "Connection must contain upgrade");
						return true;
					}
				}
				else if (header == "sec-websocket-key" && WSKey == null)
				{
					WSKey = lineString.Substring(colonIndex + 1).Trim();
					// The spec requires that this be the base64 encodding of 16 bytes, but the
					// value doesn't matter so we never check it.
				}
				else if (header == "sec-websocket-version" && !SeenWSVersionHeader)
				{
					SeenWSVersionHeader = true;
					var value = lineString.Substring(colonIndex + 1).Trim();
					if (!value.Equals("13"))
					{
						Status = WebSocketHandshakeStatus.Failed;
						SendResponse(INVALID_REQUEST, "WSVersion Must Be 13");
						return true;
					}
				}

				return false;
			}
		}
	}

	/// Determines whether the given comma-separated list contains a value, case-insensitively. The
	/// value may not contain a comma or any whitespace.
	private bool OptionInHeaderList(string header, string option)
	{
		var matchIdx = 0;
		var optionUpper = option.ToUpperInvariant();

		for (var headerIdx = 0; headerIdx < header.Length; headerIdx++)
		{
			var ch = header[headerIdx];
			if (ch == ' ' || ch == '\t' || ch == ',')
			{
				matchIdx = 0;
			}
			else if (matchIdx >= 0 && char.ToUpperInvariant(ch) == optionUpper[matchIdx])
			{
				matchIdx++;
				if (matchIdx == optionUpper.Length) return true;
			}
			else
			{
				matchIdx = -1;
			}
		}

		return false;
	}
}

/// Handles forwarding data between connected clients and the upstream server. Assumes the handshake
/// has already been performed and that any data received is WebSocket frames.
public class WebSocketServer : IManagedSocket, IDisposable
{
	/// The manager that executes our async socket requests.
	public ISocketManager Manager { get; private set; }

	/// The broker that handles client connections and processes upstream messages.
	private IBrokerServer Broker;

	/// The parser that decodes messages from the upstream server.
	private WebSocketClientParser Parser;

	/// The buffer used for decoding upstream messages, borrowed from the parser.
	private Memory<byte> FeedBuffer;

	/// The buffer used for encoding messages to send upstream
	private byte[] SendBuffer;

	/// The frame used to build messages for sending upstream
	private WebSocketFrame MessageFrame;

	/// The encoder used to serialize messages from the broker.
	private Encoder MessageEncoder;

	/// Whether we've sent a Close message or not. Once a Close message has been received, any
	/// further input can be ignored until we get a confirmation that the upstream's Close message
	/// has been replied to.
	private bool SendingClose;

	public WebSocketServer(ISocketManager manager, IBrokerServer broker)
	{
		Manager = manager;
		Broker = broker;
		Parser = new WebSocketClientParser();
		FeedBuffer = Parser.RentFeedBuffer();
		SendBuffer = ArrayPool<byte>.Shared.Rent(4096);
		MessageFrame = new WebSocketFrame();
		MessageEncoder = Encoding.UTF8.GetEncoder();
	}

	public void Dispose()
	{
		Parser.Dispose();
		ArrayPool<byte>.Shared.Return(SendBuffer);
	}

	public void OnConnected()
	{
		Broker.UpstreamConnected();
		Manager.Receive(this, FeedBuffer);
	}

	public void OnReceive(Memory<byte> destination)
	{
		Parser.Feed(destination.Length, ProcessUpstreamMessage);
		if (!SendingClose)
		{
			Manager.Receive(this, FeedBuffer);
		}
	}

	public void OnSend()
	{
		if (SendingClose)
		{
			Manager.Close(this);
		}
	}

	public void OnClose()
	{
		Broker.UpstreamDisconnected();
	}

	public void SendToUpstream(string json)
	{
		if (SendingClose) return;

		MessageFrame.IsLastFragment = true;
		MessageFrame.OpCode = MessageType.Text;
		MessageFrame.Mask = null;
		MessageFrame.Payload = MessageEncoder.GetByteCount(json, true);

		var frameSize = MessageFrame.RequiredCapacity();
		if (SendBuffer.Length < frameSize)
		{
			ArrayPool<byte>.Shared.Return(SendBuffer);
			SendBuffer = ArrayPool<byte>.Shared.Rent(frameSize);
		}

		var dataSpan = MessageFrame.WriteHeaderTo(new Span<byte>(SendBuffer));

		var charsRead = 0;
		var bytesWritten = 0;
		var completed = false;
		MessageEncoder.Convert(json, dataSpan, true, out charsRead, out bytesWritten, out completed);
		MessageFrame.MaskPayloadInBuffer(dataSpan);

		Manager.SendAll(this, new Memory<byte>(SendBuffer, 0, frameSize));
	}

	private void ProcessUpstreamMessage(WebSocketMessage message)
	{
		if (SendingClose) return;

		switch (message.Type)
		{
		case MessageType.Text:
			var command = JsonSerializer.Deserialize<EncodedCommand>(message.Text);
			if (command == null) return;
			Broker.ProcessMessage(command);
			break;

		case MessageType.Ping:
			SendPong(message.Payload);
			break;

		case MessageType.Close:
			SendClose();
			break;
		}
	}

	private void SendPong(Span<byte> payload)
	{
		MessageFrame.IsLastFragment = true;
		MessageFrame.OpCode = MessageType.Pong;
		MessageFrame.Mask = null;
		MessageFrame.Payload = payload.Length;

		var frameSize = MessageFrame.RequiredCapacity();
		var dataSpan = MessageFrame.WriteHeaderTo(new Span<byte>(SendBuffer));

		payload.CopyTo(dataSpan);
		MessageFrame.MaskPayloadInBuffer(dataSpan);
		Manager.SendAll(this, new Memory<byte>(SendBuffer, 0, frameSize));
	}

	private void SendClose()
	{
		if (SendingClose) return;
		SendingClose = true;

		MessageFrame.IsLastFragment = true;
		MessageFrame.OpCode = MessageType.Close;
		MessageFrame.Mask = null;
		MessageFrame.Payload = 0;

		var frameSize = MessageFrame.RequiredCapacity();
		MessageFrame.WriteHeaderTo(new Span<byte>(SendBuffer));
		Manager.SendAll(this, new Memory<byte>(SendBuffer, 0, frameSize));
	}
}
