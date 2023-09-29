// -*- mode: csharp; fill-column: 100 -*-
using System.Buffers;
using System.Net;
using System.Text;

public abstract class TestUtil
{
	/// Creates a new array and fills it with random bytes.
	protected byte[] FillRandomBytes(int size)
	{
		var body = new byte[size];
		Random.Shared.NextBytes(body);
		return body;
	}

	/// Creates a new array and fills it with random ASCII text.
	protected byte[] FillRandomASCII(int size)
	{
		var body = new byte[size];
		Random.Shared.NextBytes(body);

		var range = '~' - ' ';
		for (var i = 0; i < body.Length; i++)
		{
			var offset = body[i] % range;
			body[i] = (byte)(' ' + offset);
		}
		return body;
	}

	/// Chunks a buffer into pieces with the given maximum size.
	protected IEnumerable<Memory<byte>> ChunkBuffer(byte[] buffer, int chunkSize)
	{
		var offset = 0;
		while (offset < buffer.Length)
		{
			var thisChunk = Math.Min(buffer.Length - offset, chunkSize);
			yield return new Memory<byte>(buffer, offset, thisChunk);
			offset += thisChunk;
		}
	}

	/// Sends a binary message to the given broker connection.
	protected void SendBytesAsBroker(IBrokerConnection connection, byte[] message)
	{
		var capacity = connection.MessageCapacity(message.Length);
		var buffer = ArrayPool<byte>.Shared.Rent(capacity);
		var messageDest = new Span<byte>(buffer, capacity - message.Length, message.Length);
		new Span<byte>(message).CopyTo(messageDest);

		var messageSegment = new ArraySegment<byte>(buffer, 0, capacity);
		connection.SendMessage(messageSegment, message.Length);
	}

	/// Creates an array segment of the given string, encoding it to ASCII
	protected ArraySegment<byte> ArrayOfString(string message)
	{
		return new ArraySegment<byte>(Encoding.ASCII.GetBytes(message));
	}

	/// Writes the given broker command to a new array
	protected byte[] EncodeBrokerCommand(BrokerCommand command)
	{
		var buffer = new byte[command.EncodedSize()];
		command.EncodeTo(new ArraySegment<byte>(buffer));
		return buffer;
	}
}

public enum ManagerEvents
{
	Receive,
	Send,
	Close,
}

public class DummySocketHandle
{
	public ManagedSocketBase<DummySocketHandle>? Socket;
	public Queue<Memory<byte>> ReceiveQueue = new Queue<Memory<byte>>();
	public ArraySegment<byte>? NextBuffer;
	public MemoryStream Output = new MemoryStream();
}

public class DummyManager : ISocketManager<DummySocketHandle>
{
	private static readonly Memory<byte> EMPTY = new Memory<byte>(new byte[0]);
	public DummySocketHandle Handle = new DummySocketHandle();
	public bool RecvPending;

	public void EnqueueReceive(Memory<byte> buffer)
	{
		Handle.ReceiveQueue.Enqueue(buffer);
	}

	public void EnqueueEmpty()
	{
		Handle.ReceiveQueue.Enqueue(EMPTY);
	}

	public byte[] RawOutput()
	{
		return Handle.Output.ToArray();
	}

	public string DecodeOutput()
	{
		return Encoding.UTF8.GetString(Handle.Output.ToArray());
	}

	public bool AllDataReceived()
	{
		return Handle.ReceiveQueue.Count == 0;
	}

	public bool CanStep()
	{
		return Handle.ReceiveQueue.Count > 0;
	}

	public void DirectBind(ManagedSocketBase<DummySocketHandle> socket)
	{
		socket.ManagerHandle = Handle;
		Handle.Socket = socket;
	}

	public void Step()
	{
		if (!RecvPending)
		{
			throw new Exception("Cannot step a manager with no pending receives");
		}

		if (Handle.Socket == null) throw new Exception("No socket attached to handle");
		if (Handle.NextBuffer == null) throw new Exception("No buffer attached to handle");
		Handle.Socket.OnReceive(Handle.NextBuffer.Value);
	}

	public EndPoint Bind(EndPoint address, ManagedSocketFactory<DummySocketHandle> factory)
	{
		// Unused in the tests - the socket is injected directly via the DirectBind operation
		return address;
	}

	public void Receive(DummySocketHandle socket, ArraySegment<byte> destination)
	{
		if (socket.ReceiveQueue.Count == 0)
		{
			throw new Exception("Receive: no data pending");
		}

		var buffer = socket.ReceiveQueue.Dequeue();
		if (buffer.Length > destination.Count)
		{
			throw new Exception($"Receive: {buffer.Length} byte buffer does not fit in {destination.Count} byte slice");
		}

		buffer.CopyTo(destination);

		// Don't call OnReceive directly because that could lead to a stack overflow if the
		// number of receives is big enough
		Handle.NextBuffer = destination.Slice(0, buffer.Length);
		RecvPending = true;
	}

	public void SendAll(DummySocketHandle socket, ArraySegment<byte> source)
	{
		if (socket.Socket == null) throw new Exception("No socket attached to handle");
		socket.Output.Write(source.AsSpan());
		socket.Socket.OnSend();
	}

	public void Close(DummySocketHandle socket)
	{
		if (socket.Socket == null) throw new Exception("No socket attached to handle");
		socket.Socket.OnClose();
	}

    public void ChangeHandler(DummySocketHandle socket, ManagedSocketBase<DummySocketHandle> newSocket)
    {
		DirectBind(newSocket);
		newSocket.OnConnected();
    }
}

public class DummyServerBroker : IBrokerServer
{
	public List<string> Commands = new List<string>();
	public List<int> Targets = new List<int>();
	public bool IsConnected;

	public void UpstreamConnected(IBrokerConnection connection)
	{
		IsConnected = true;
	}

	public void UpstreamDisconnected()
	{
		IsConnected = false;
	}

	public void ForwardToClient(int target, ArraySegment<byte> message)
	{
		Targets.Add(target);
		Commands.Add(Encoding.ASCII.GetString(message));
	}
}

public class DummyClientHandle : IEquatable<DummyClientHandle>
{
	private static int IdGenerator = 0;

	public static void ResetIdGenerator()
	{
		IdGenerator = 0;
	}

	public string Id;
	public string Name;
	public List<string> Fragments = new List<string>();

	public DummyClientHandle(string name)
	{
		Id = IdGenerator.ToString();
		IdGenerator++;
		Name = name;
	}

    public bool Equals(DummyClientHandle? other)
    {
		if (other == null) return false;
		return this.Id == other.Id;
    }
}

public class DummyClientBroker : IBrokerClient<DummyClientHandle>
{
	public ISet<DummyClientHandle> RegisteredClients = new HashSet<DummyClientHandle>();

    public void ForwardToServer(DummyClientHandle client, ArraySegment<byte> message)
    {
		if (client == null)
		{
			throw new ArgumentException("Cannot unregister null client");
		}

		client.Fragments.Add(Encoding.ASCII.GetString(message));
    }

    public DummyClientHandle RegisterClient(IBrokerConnection connection, ArraySegment<byte> name)
    {
		var handle = new DummyClientHandle(Encoding.ASCII.GetString(name));
		RegisteredClients.Add(handle);
		return handle;
    }

    public void UnregisterClient(DummyClientHandle client)
    {
		if (client == null)
		{
			throw new ArgumentException("Cannot unregister null client");
		}

		if (!RegisteredClients.Contains(client))
		{
			throw new ArgumentException($"Cannot unregister non-existent client {client.Id}");
		}

		RegisteredClients.Remove(client);
    }
}
