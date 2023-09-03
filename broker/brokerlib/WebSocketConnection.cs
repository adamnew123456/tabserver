// -*- mode: csharp; fill-column: 100 -*-
using System.Security.Cryptography;
using System.IO;
using System.Text;

namespace brokerlib;

/// Handles the initial handshake of the WebSocket session. Waits for an HTTP/1.1 GET, validates the
/// required headers, and then transitions to the WebSocketMessageState.
public class WebSocketHandshake<SocketHandleT> : ManagedSocketBase<SocketHandleT>
{
	private enum HandshakeStatus
	{
		/// The handshake hasn't received the client's full request yet
		Pending,

		/// The handshake request was valid and the success response has been sent
		Successful,

		/// The handshake request was invalid and the failure response has been sent
		Failed,
	}


    private const int BUFFER_SIZE = 4096;

	/// HTTP status codes for errors that occur when processing the request
	private const int INVALID_REQUEST = 400;
	private const int NOT_FOUND = 404;
	private const int METHOD_NOT_ALLOWED = 405;
	private const int NOT_IMPLEMENTED = 501;

	/// The broker to pass along to the main connection handler
	public IBrokerServer Broker { private get; set; }

    /// Buffer used for storing data from the socket
    private ReceiveBuffer ReceiveBuffer = new ReceiveBuffer(BUFFER_SIZE);

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
	private HandshakeStatus Status;

	public WebSocketHandshake(ISocketManager<SocketHandleT> manager, IBrokerServer broker)
	{
		Manager = manager;
		Broker = broker;
		Status = HandshakeStatus.Pending;
	}

	public override void OnConnected()
	{
		Manager.Receive(ManagerHandle, ReceiveBuffer.WritableSlice());
	}

	public override void OnReceive(ArraySegment<byte> destination)
	{
		var foundReturn = false;
		var lineStart = 0;
		var buffer = ReceiveBuffer.ReadableSlice(destination.Count).AsSpan();

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
			Status = HandshakeStatus.Failed;
			SendResponse(INVALID_REQUEST, "Line Too Long");
			return;
		}

		ReceiveBuffer.SaveUnread(lineStart);
		Manager.Receive(ManagerHandle, ReceiveBuffer.WritableSlice());
	}

	public override void OnSend()
	{
		if (Status == HandshakeStatus.Successful)
		{
			Manager.ChangeHandler(ManagerHandle, new WebSocketServer<SocketHandleT>(Manager, Broker));
		}
	}

	public override void OnClose()
	{
	}

	/// Constructs a response value to send to the client, once the request has been completely
	/// processed. The status code and details are only used when sending error responses.
	private void SendResponse(int status = 0, string? details = null)
	{
		string message;
		if (Status == HandshakeStatus.Failed)
		{
			message = $"HTTP/1.1 {status} {details ?? "Unknown"}\r\n";
		}
		else
		{
			var hashedKey = Convert.ToBase64String(SHA1.HashData(Encoding.UTF8.GetBytes($"{WSKey}{WebSocketProto.HANDSHAKE_KEY}")));
			message = $"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {hashedKey}\r\n\r\n";
		}

		var data = Encoding.UTF8.GetBytes(message);
		Manager.SendAll(ManagerHandle, new ArraySegment<byte>(data));
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
				Status = HandshakeStatus.Failed;
				SendResponse(INVALID_REQUEST, "Not HTTP11 Request Line");
				return true;
			}

			if (parts[0] != "GET")
			{
				Status = HandshakeStatus.Failed;
				SendResponse(METHOD_NOT_ALLOWED, "Only GET Allowed");
				return true;
			}

			if (parts[1] != "/")
			{
				Status = HandshakeStatus.Failed;
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
					Status = HandshakeStatus.Failed;
					SendResponse(INVALID_REQUEST, "Missing Header");
				}
				else
				{
					Status = HandshakeStatus.Successful;
					SendResponse();
				}

				return true;
			}
			else if (line[0] == ' ' || line[0] == '\t')
			{
				Status = HandshakeStatus.Failed;
				SendResponse(NOT_IMPLEMENTED, "Unsupported Header Continuation");
				return true;
			}
			else
			{
				var lineString = Encoding.ASCII.GetString(line);
				int colonIndex = lineString.IndexOf(':');
				if (colonIndex == -1)
				{
					Status = HandshakeStatus.Failed;
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
						Status = HandshakeStatus.Failed;
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
						Status = HandshakeStatus.Failed;
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
						Status = HandshakeStatus.Failed;
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
public class WebSocketServer<SocketHandleT> : ManagedSocketBase<SocketHandleT>, IBrokerConnection, IDisposable
{
	private struct PendingMessage
	{
		/// The full buffer passed to SendMessage, including the space for the WebSocket header.
		public ArraySegment<byte> Buffer { get; set; }

		/// The size of the input message without any WebSocket data.
		public int DataSize { get; set; }

		/// The type of message being sent
		public MessageType OpCode { get; set; }
	}

    /// The broker that handles client connections and processes upstream messages.
    private IBrokerServer Broker;

	/// The parser that decodes messages from the upstream server.
	private WebSocketClientParser Parser;

	/// The buffer used for decoding upstream messages, borrowed from the parser.
	private ArraySegment<byte> FeedBuffer;

	/// The frame used to build messages for sending upstream
	private WebSocketFrame MessageFrame;

	/// Whether we've sent a Close message or not. Once a Close message has been received, any
	/// further input can be ignored until we get a confirmation that the upstream's Close message
	/// has been replied to.
	private bool SendingClose;

    /// Tracks the buffers for pending messages.
    private Queue<PendingMessage> PendingSend;

	public WebSocketServer(ISocketManager<SocketHandleT> manager, IBrokerServer broker)
	{
		Manager = manager;
		Broker = broker;
		Parser = new WebSocketClientParser();
		FeedBuffer = Parser.RentFeedBuffer();
		MessageFrame = new WebSocketFrame();
        PendingSend = new Queue<PendingMessage>();
	}

	public void Dispose()
	{
		Parser.Dispose();
		while (PendingSend.Count > 0)
		{
			var message = PendingSend.Dequeue();
			ArrayPool<byte>.Shared.Return(message.Buffer.Array);
		}
		GC.SuppressFinalize(this);
	}

	public override void OnConnected()
	{
		Broker.UpstreamConnected(this);
		Manager.Receive(ManagerHandle, FeedBuffer);
	}

	public override void OnReceive(ArraySegment<byte> destination)
	{
		Parser.Feed(destination.Count, ProcessUpstreamMessage);
		if (!SendingClose)
		{
			Manager.Receive(ManagerHandle, FeedBuffer);
		}
	}

	public override void OnSend()
	{
		if (SendingClose)
		{
			Manager.Close(ManagerHandle);
			return;
		}

		lock (PendingSend)
		{
			if (PendingSend.Count > 0)
			{
				var lastSent = PendingSend.Dequeue();
				ArrayPool<byte>.Shared.Return(lastSent.Buffer.Array);
			}
		}

        SendPendingMessage();
	}

	public override void OnClose()
	{
		Broker.UpstreamDisconnected();
	}

	private void SendPendingMessage()
	{
		lock (PendingSend)
		{
			if (PendingSend.Count > 0)
			{
				var message = PendingSend.Dequeue();
				SendingClose = message.OpCode == MessageType.Close;

				MessageFrame.IsLastFragment = true;
				MessageFrame.OpCode = message.OpCode;
				MessageFrame.Mask = null;
				MessageFrame.Payload = message.DataSize;

				var payload = MessageFrame.WriteHeaderTo(new ArraySegment<byte>(message.Buffer.Array, message.Buffer.Offset, message.Buffer.Count));
				MessageFrame.MaskPayloadInBuffer(payload);
				Manager.SendAll(ManagerHandle, new ArraySegment<byte>(message.Buffer.Array, message.Buffer.Offset, message.Buffer.Count));
			}
		}
	}

	public int MessageCapacity(int messageBytes)
	{
		MessageFrame.IsLastFragment = true;
		MessageFrame.OpCode = MessageType.Binary;
		MessageFrame.Mask = null;
		MessageFrame.Payload = messageBytes;
		return MessageFrame.RequiredCapacity();
	}

	public void SendMessage(ArraySegment<byte> buffer, int messageBytes)
	{
		if (SendingClose) return;

		lock (PendingSend)
		{
			PendingSend.Enqueue(new PendingMessage()
			{
				Buffer = buffer,
				DataSize = messageBytes,
				OpCode = MessageType.Binary
			});
			if (PendingSend.Count == 1)
			{
				// OnSend will continue pumping the queue as long as it is not empty, but we need to
				// prime it if it was.
				SendPendingMessage();
			}
		}
	}

	public override void Close()
	{
		SendClose();
	}

	private void ProcessUpstreamMessage(WebSocketMessage message)
	{
		if (SendingClose) return;

		switch (message.Type)
		{
		case MessageType.Binary:
			var command = BrokerCommand.Decode(message.Payload);
			if (command is SendBrokerCommand send)
			{
				Broker.ForwardToClient(send.Id, send.Command);
			}
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
		MessageFrame.OpCode = MessageType.Ping;
		MessageFrame.Mask = null;
		MessageFrame.Payload = payload.Length;
		var capacity = MessageCapacity(payload.Length);

		var buffer = ArrayPool<byte>.Shared.Rent(capacity);
		var payloadSpan = new Span<byte>(buffer, capacity - payload.Length, payload.Length);
		payload.CopyTo(payloadSpan);

		var bufferSegment = new ArraySegment<byte>(buffer, 0, capacity);
		lock (PendingSend)
		{
			PendingSend.Enqueue(new PendingMessage()
			{
				Buffer = bufferSegment,
				DataSize = payload.Length,
				OpCode = MessageType.Pong
			});
			if (PendingSend.Count == 1)
			{
				SendPendingMessage();
			}
		}
	}

	private void SendClose()
	{
		MessageFrame.IsLastFragment = true;
		MessageFrame.OpCode = MessageType.Close;
		MessageFrame.Mask = null;
		MessageFrame.Payload = 0;
		var capacity = MessageCapacity(0);

		var buffer = ArrayPool<byte>.Shared.Rent(capacity);
		var bufferSegment = new ArraySegment<byte>(buffer, 0, capacity);
		lock (PendingSend)
		{
			PendingSend.Enqueue(new PendingMessage()
			{
				Buffer = bufferSegment,
				DataSize = 0,
				OpCode = MessageType.Close
			});
			if (PendingSend.Count == 1)
			{
				SendPendingMessage();
			}
		}
	}
}
