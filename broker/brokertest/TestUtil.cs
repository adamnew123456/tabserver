// -*- mode: csharp; fill-column: 100 -*-
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
}

public enum ManagerEvents
{
	Receive,
	Send,
	Close,
}

public class DummyManager : ISocketManager
{
	private Queue<Memory<byte>> ReceiveQueue = new Queue<Memory<byte>>();
	private MemoryStream Output = new MemoryStream();
	private IManagedSocket? NextReceiveSocket;
	private Memory<byte>? NextReceiveBuffer;

	public void EnqueueReceive(Memory<byte> buffer)
	{
		ReceiveQueue.Enqueue(buffer);
	}

	public byte[] RawOutput()
	{
		return Output.ToArray();
	}

	public string DecodeOutput()
	{
		return Encoding.UTF8.GetString(Output.ToArray());
	}

	public bool AllDataReceived()
	{
		return ReceiveQueue.Count == 0;
	}

	public void Step()
	{
		if (NextReceiveBuffer != null)
		{
			NextReceiveSocket.OnReceive(NextReceiveBuffer.Value);
		}
	}

	public void Receive(IManagedSocket socket, Memory<byte> destination)
	{
		if (ReceiveQueue.Count == 0)
		{
			throw new Exception("Receive: no data pending");
		}

		var buffer = ReceiveQueue.Dequeue();
		if (buffer.Length > destination.Length)
		{
			throw new Exception($"Receive: {buffer.Length} byte buffer does not fit in {destination.Length} byte slice");
		}

		buffer.CopyTo(destination);

		// Don't call OnReceive directly because that could lead to a stack overflow if the
		// number of receives is big enough
		NextReceiveSocket = socket;
		NextReceiveBuffer = destination.Slice(0, buffer.Length);
	}

	public void SendAll(IManagedSocket socket, Memory<byte> source)
	{
		Output.Write(source.Span);
		socket.OnSend();
	}

	public void Close(IManagedSocket socket)
	{
		socket.OnClose();
	}
}

public class DummyServerBroker : IBrokerServer
{
	public List<EncodedCommand> Commands = new List<EncodedCommand>();
	public bool IsConnected;

	public void UpstreamConnected()
	{
		IsConnected = true;
	}

	public void UpstreamDisconnected()
	{
		IsConnected = false;
	}

	public void ProcessMessage(EncodedCommand command)
	{
		Commands.Add(command);
	}
}
