// -*- mode: csharp; fill-column: 100 -*-
using System.Text;

namespace brokerlib;

public class TabServer<ClientHandle> : IManagedSocket, IBrokerConnection, IDisposable
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

    private const int BUFFER_SIZE = 4096;

	/// The manager that executes our async socket requests.
	public ISocketManager Manager { get; private set; }

	/// The broker that handles client connections and forwards requests to the usptream connection.
	private IBrokerClient<ClientHandle> Broker;

    /// Storage used to buffer the current line of input from the server.
    private ReceiveBuffer ReceiveBuffer;

    /// What next line we're expecting, if any. Used to capture the HELLO message.
    private HelloState State;

    /// The unique handle for our connected client.
    private ClientHandle? Handle;

    /// Decoder used to read ASCII bytes into strings
    private Decoder LineDecoder;

    /// Buffer used for decoding messages into strings
    private char[] LineBuffer;

    /// Tracks the buffers for pending messages.
    private Queue<ArraySegment<byte>> PendingSend;

    public TabServer(ISocketManager manager, IBrokerClient<ClientHandle> broker)
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

    public void OnConnected()
    {
        Manager.Receive(this, ReceiveBuffer.WritableSlice());
    }

    public void OnReceive(Memory<byte> destination)
    {
        var buffer = ReceiveBuffer.ReadableSlice(destination.Length).Span;
        var lineStart = 0;
        var i = 0;

        while (i < buffer.Length)
        {
            switch (State)
            {
            case HelloState.AwaitHello:
                if (buffer.Length < 6) goto doneReading;
                // HELLO is always the first thing the client transmits, so we can assume it'll be
                // at the start of the buffer
                if (buffer[0] != 'H'
                    || buffer[1] != 'E'
                    || buffer[2] != 'L'
                    || buffer[3] != 'L'
                    || buffer[4] != 'O'
                    || buffer[5] != '\n')
                {
                    Manager.Close(this);
                    return;
                }

                State = HelloState.AwaitIntro;
                i = 6;
                lineStart = 6;
                break;

            case HelloState.AwaitIntro:
                if (buffer[i] == '\n')
                {
                    var bytesRead = 0;
                    var charsWritten = 0;
                    var completed = false;
                    var outputBuffer = new Span<char>(LineBuffer);
                    LineDecoder.Convert(buffer.Slice(lineStart, i - lineStart),
                                        outputBuffer,
                                        true,
                                        out bytesRead,
                                        out charsWritten,
                                        out completed);

                    var client = new String(outputBuffer.Slice(0, charsWritten));
                    Handle = Broker.RegisterClient(client);
                    lineStart = i + 1;
                    State = HelloState.Forward;
                }

                i++;
                break;

            case HelloState.Forward:
                if (buffer[i] == '\n')
                {
                    var bytesRead = 0;
                    var charsWritten = 0;
                    var completed = false;
                    var outputBuffer = new Span<char>(LineBuffer);
                    LineDecoder.Convert(buffer.Slice(lineStart, i - lineStart),
                                        outputBuffer,
                                        true,
                                        out bytesRead,
                                        out charsWritten,
                                        out completed);

                    var message = new String(outputBuffer.Slice(0, charsWritten));
                    Broker.ForwardToServer(Handle, message, true);
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
            // Don't bother buffering this - there is no *strict* limit on the length of the HELLO
            // message, but in practice it will never be this long.
            Manager.Close(this);
            return;
        }

        if (State == HelloState.Forward && lineStart < buffer.Length)
        {
            var fragmentLength = buffer.Length - lineStart;

            var bytesRead = 0;
            var charsWritten = 0;
            var completed = false;
            var outputBuffer = new Span<char>(LineBuffer);
            LineDecoder.Convert(buffer.Slice(lineStart, fragmentLength),
                                outputBuffer,
                                true,
                                out bytesRead,
                                out charsWritten,
                                out completed);

            var message = new String(outputBuffer.Slice(0, charsWritten));
            Broker.ForwardToServer(Handle, message, false);
            ReceiveBuffer.SaveUnread(buffer.Length);
        }
        else
        {
            ReceiveBuffer.SaveUnread(lineStart);
        }

        Manager.Receive(this, ReceiveBuffer.WritableSlice());
    }

    public void OnSend()
    {
        if (PendingSend.Count > 0)
        {
            var lastSent = PendingSend.Dequeue();
            ArrayPool<byte>.Shared.Return(lastSent.Array);
        }

        SendPendingMessage();
    }

    private void SendPendingMessage()
    {
        if (PendingSend.Count > 0)
        {
            // Don't dequeue it yet, we have to keep the buffer alive until the async send
            // completes.
            var buffer = PendingSend.Peek();
            Manager.SendAll(this, new Memory<byte>(buffer.Array, buffer.Offset, buffer.Count));
        }
    }

    public void OnClose()
    {
        if (State == HelloState.Forward)
        {
            Broker.UnregisterClient(Handle);
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
}
