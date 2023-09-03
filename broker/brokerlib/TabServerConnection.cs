// -*- mode: csharp; fill-column: 100 -*-
using System.Text;

namespace brokerlib;

public class TabServer<BrokerHandleT, SocketHandleT> : ManagedSocketBase<SocketHandleT>, IBrokerConnection, IDisposable
{
    private enum HelloState
    {
        /// We're waiting for the HELLO line
        AwaitHello,
        /// We're waiting for the line after the HELLO
        AwaitIntro,
        /// We're all lines to the upstream
        Forward,
    }

    /// The maximum size of a HELLO message
    private const int BUFFER_SIZE = UInt16.MaxValue;

    /// The broker that handles client connections and forwards requests to the usptream connection.
    private IBrokerClient<BrokerHandleT> Broker;

    /// Storage used to buffer the current line of input from the server.
    private ReceiveBuffer ReceiveBuffer;

    /// What next line we're expecting, if any. Used to capture the HELLO message.
    private HelloState State;

    /// The unique handle for our connected client inside the broker.
    private BrokerHandleT? BrokerHandle;

    /// Decoder used to read ASCII bytes into strings
    private Decoder LineDecoder;

    /// Buffer used for decoding messages into strings
    private char[] LineBuffer;

    /// Tracks the buffers for pending messages.
    private Queue<ArraySegment<byte>> PendingSend;

    public TabServer(ISocketManager<SocketHandleT> manager, IBrokerClient<BrokerHandleT> broker)
    {
        Manager = manager;
        Broker = broker;
        State = HelloState.AwaitHello;
        ReceiveBuffer = new ReceiveBuffer(BUFFER_SIZE);
        LineDecoder = Encoding.ASCII.GetDecoder();
        LineBuffer = ArrayPool<char>.Shared.Rent(BUFFER_SIZE);
        PendingSend = new Queue<ArraySegment<byte>>();
    }

	public void Dispose()
	{
        ReceiveBuffer.Dispose();
        ArrayPool<char>.Shared.Return(LineBuffer);
        while (PendingSend.Count > 0)
        {
            ArrayPool<byte>.Shared.Return(PendingSend.Dequeue().Array);
        }
		GC.SuppressFinalize(this);
	}

    public override void OnConnected()
    {
        Manager.Receive(ManagerHandle, ReceiveBuffer.WritableSlice());
    }

    public override void OnReceive(ArraySegment<byte> destination)
    {
        var buffer = ReceiveBuffer.ReadableSlice(destination.Count);
        var lineStart = 0;
        var i = 0;

        while (i < buffer.Count && State != HelloState.Forward)
        {
            switch (State)
            {
            case HelloState.AwaitHello:
                if (buffer.Count < 6) goto doneReading;
                // HELLO is always the first thing the client transmits, so we can assume it'll be
                // at the start of the buffer
                if (buffer[0] != 'H'
                    || buffer[1] != 'E'
                    || buffer[2] != 'L'
                    || buffer[3] != 'L'
                    || buffer[4] != 'O'
                    || buffer[5] != '\n')
                {
                    Manager.Close(ManagerHandle);
                    return;
                }

                State = HelloState.AwaitIntro;
                i = 6;
                lineStart = 6;
                break;

            case HelloState.AwaitIntro:
                if (buffer[i] == '\n')
                {
                    var client = buffer.Slice(lineStart, i - lineStart);
                    BrokerHandle = Broker.RegisterClient(this, client);
                    lineStart = i + 1;
                    State = HelloState.Forward;
                }

                i++;
                break;
            }
        }

    doneReading:
        if ((State == HelloState.AwaitHello || State == HelloState.AwaitIntro)
            && ReceiveBuffer.IsFull)
        {
            // Don't bother buffering this - the HELLO message won't fit into our broker message
            // frames, so we can't send it to the upstream.
            Manager.Close(ManagerHandle);
            return;
        }

        if (State == HelloState.Forward && lineStart < buffer.Count)
        {
            var message = buffer.Slice(lineStart, buffer.Count - lineStart);
            Broker.ForwardToServer(BrokerHandle, message);
            ReceiveBuffer.SaveUnread(buffer.Count);
        }
        else
        {
            ReceiveBuffer.SaveUnread(lineStart);
        }

        Manager.Receive(ManagerHandle, ReceiveBuffer.WritableSlice());
    }

    public override void OnSend()
    {
        lock (PendingSend)
        {
            if (PendingSend.Count > 0)
            {
                var lastSent = PendingSend.Dequeue();
                ArrayPool<byte>.Shared.Return(lastSent.Array);
            }
        }

        SendPendingMessage();
    }

    public override void OnClose()
    {
        if (State == HelloState.Forward)
        {
            Broker.UnregisterClient(BrokerHandle);
        }
    }

    public int MessageCapacity(int messageBytes)
    {
        return messageBytes;
    }

	public void SendMessage(ArraySegment<byte> buffer, int messageBytes)
	{
        PendingSend.Enqueue(buffer);
        if (PendingSend.Count == 1)
        {
            // OnSend will continue pumping the queue as long as it is not empty, but we need to
            // prime it if it was.
            OnSend();
        }
	}

    private void SendPendingMessage()
    {
        lock (PendingSend)
        {
            if (PendingSend.Count > 0)
            {
                // Don't dequeue it yet, we have to keep the buffer alive until the async send
                // completes.
                var buffer = PendingSend.Peek();
                Manager.SendAll(ManagerHandle, new ArraySegment<byte>(buffer.Array, buffer.Offset, buffer.Count));
            }
        }
    }
}
