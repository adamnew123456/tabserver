// -*- mode: csharp; fill-column: 100 -*-
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;

namespace brokerlib;

public abstract class BrokerCommand : IEquatable<BrokerCommand>
{
    protected const int HELLO_COMMAND = 0;
    protected const int GOODBYE_COMMAND = 1;
    protected const int SEND_COMMAND = 2;

    public static BrokerCommand Decode(Span<byte> buffer)
    {
        if (buffer.Length == 0)
        {
            throw new InvalidDataException("Command buffer is empty");
        }

        var opcode = buffer[0];

        switch (opcode)
        {
            case HELLO_COMMAND:
            case SEND_COMMAND:
                {
                    // Minimum: opcode (1), ID (4), content (2 + data)
                    if (buffer.Length < 7)
                    {
                        throw new InvalidDataException($"Command buffer is not large enough. Minimum is 7 bytes.");
                    }

                    var bodyLength = BitConverter.ToUInt16(buffer.Slice(5));

                    if (buffer.Length < 7 + bodyLength)
                    {
                        throw new InvalidDataException($"Command buffer is not large enough. Minimum is {7 + bodyLength} bytes.");
                    }

                    var id = BitConverter.ToInt32(buffer.Slice(1));
                    var body = buffer.Slice(7, bodyLength).ToArray();

                    if (opcode == HELLO_COMMAND)
                    {
                        return new HelloBrokerCommand()
                        {
                            Id = id,
                            Name = new ArraySegment<byte>(body),
                        };
                    }
                    else
                    {
                        return new SendBrokerCommand()
                        {
                            Id = id,
                            Command = new ArraySegment<byte>(body),
                        };
                    }
                }
            case GOODBYE_COMMAND:
                {
                    // Minimum: opcode (1), ID (4)
                    if (buffer.Length < 5)
                    {
                        throw new InvalidDataException($"Command buffer is not large enough. Minimum is 5 bytes.");
                    }

                    var id = BitConverter.ToInt32(buffer.Slice(1));
                    return new GoodbyeBrokerCommand()
                    {
                        Id = id,
                    };
                }
            default:
                throw new InvalidDataException($"Unexpected operation with code {opcode:x}");
        }
    }

    protected bool BufferEquals(ArraySegment<byte> a, ArraySegment<byte> b)
    {
        if (a.Count != b.Count) return false;
        for (var i = 0; i < a.Count; i++)
        {
            if (a[i] != b[i]) return false;
        }
        return true;
    }

    public abstract int EncodedSize();
    public abstract void EncodeTo(ArraySegment<byte> buffer);
    public abstract bool Equals(BrokerCommand? command);
}

public class HelloBrokerCommand : BrokerCommand
{
    public int Id { get; set; }
    public ArraySegment<byte> Name { get; set; }

    public override int EncodedSize()
    {
        return 7 + Name.Count;
    }

    public override void EncodeTo(ArraySegment<byte> buffer)
    {
        buffer[0] = HELLO_COMMAND;
        BitConverter.TryWriteBytes(buffer.Slice(1), Id);
        BitConverter.TryWriteBytes(buffer.Slice(5), (ushort)Name.Count);
        Name.CopyTo(buffer.Slice(7));
    }

    public override bool Equals(BrokerCommand? command)
    {
        if (command is HelloBrokerCommand hello)
        {
            return this.Id == hello.Id && BufferEquals(this.Name, hello.Name);
        }
        return false;
    }
}

public class GoodbyeBrokerCommand : BrokerCommand
{
    public int Id { get; set; }

    public override int EncodedSize()
    {
        return 5;
    }

    public override void EncodeTo(ArraySegment<byte> buffer)
    {
        buffer[0] = GOODBYE_COMMAND;
        BitConverter.TryWriteBytes(buffer.Slice(1), Id);
    }

    public override bool Equals(BrokerCommand? command)
    {
        if (command is GoodbyeBrokerCommand goodbye)
        {
            return this.Id == goodbye.Id;
        }
        return false;
    }
}

public class SendBrokerCommand : BrokerCommand
{
    public int Id { get; set; }
    public ArraySegment<byte> Command { get; set; }

    public override int EncodedSize()
    {
        return 7 + Command.Count;
    }

    public override void EncodeTo(ArraySegment<byte> buffer)
    {
        buffer[0] = SEND_COMMAND;
        BitConverter.TryWriteBytes(buffer.Slice(1), Id);
        BitConverter.TryWriteBytes(buffer.Slice(5), (ushort)Command.Count);
        Command.CopyTo(buffer.Slice(7));
    }

    public override bool Equals(BrokerCommand? command)
    {
        if (command is SendBrokerCommand send)
        {
            return this.Id == send.Id && BufferEquals(this.Command, send.Command);
        }
        return false;
    }
}

public abstract class BrokerEvent
{
}

public class StopBroker : BrokerEvent
{
    public static readonly StopBroker Instance = new StopBroker();
    private StopBroker() { }
}

public class UpstreamConnected : BrokerEvent
{
    public BrokerHandle Handle { get; set; }
}

public class UpstreamDisconnected : BrokerEvent
{
    public static readonly UpstreamDisconnected Instance = new UpstreamDisconnected();
    private UpstreamDisconnected() { }
}

public class ClientConnected : BrokerEvent
{
    public BrokerHandle Client { get; set; }
    public ArraySegment<byte> Name { get; set; }
}

public class ClientDisconnected : BrokerEvent
{
    public BrokerHandle Client { get; set; }
}

public class ForwardToClient : BrokerEvent
{
    public int DestinationClient { get; set; }
    public ArraySegment<byte> Message { get; set; }
}

public class ForwardToUpstream : BrokerEvent
{
    public BrokerHandle SourceClient { get; set; }
    public ArraySegment<byte> Message { get; set; }
}

public class BrokerHandle : IDisposable
{
    private static int IdGenerator = 0;
    internal int Id;
    internal IBrokerConnection Connection;
    internal ArraySegment<byte> MessageBuffer;

    public BrokerHandle(IBrokerConnection connection)
    {
        Id = Interlocked.Increment(ref IdGenerator);
        Connection = connection;
        MessageBuffer = ArraySegment<byte>.Empty;
    }

    public ArraySegment<byte> PrepareSend(int size)
    {
        var totalSize = Connection.MessageCapacity(size);
        var buffer = ArrayPool<byte>.Shared.Rent(totalSize);
        MessageBuffer = new ArraySegment<byte>(buffer, 0, totalSize);
        return new ArraySegment<byte>(buffer, totalSize - size, size);
    }

    public void Send(int size)
    {
        Connection.SendMessage(MessageBuffer, size);
        MessageBuffer = ArraySegment<byte>.Empty;
    }

    public void Close()
    {
        Connection.Close();
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        if (MessageBuffer.Count > 0)
        {
            ArrayPool<byte>.Shared.Return(MessageBuffer.Array);
        }
    }
}

public class BrokerEventDispatcher
{
    private enum UpstreamState
    {
        NoConnection,
        WaitingForHandshake,
        Connected,
    }

    private class StartArgs
    {
        public AsyncSocketManager Manager { get; set; }
        public EndPoint Upstream { get; set; }
        public EndPoint Client { get; set; }
    }

    private AutoResetEvent EventWaiter;
    private ConcurrentQueue<BrokerEvent> Events;
    private UpstreamState State;
    private ManagedSocketBase<AsyncSocketHandle>? UpstreamSocket;
    private BrokerHandle? UpstreamBroker;
    private Dictionary<int, BrokerHandle> Clients;
    private AsyncSocketManager? Manager;
    private BrokerServerEvents ServerAdapter;
    private BrokerClientEvents ClientAdapter;

    public BrokerEventDispatcher()
    {
        EventWaiter = new AutoResetEvent(false);
        Events = new ConcurrentQueue<BrokerEvent>();
        State = UpstreamState.NoConnection;
        UpstreamBroker = null;
        Clients = new Dictionary<int, BrokerHandle>();
        ServerAdapter = new BrokerServerEvents(this);
        ClientAdapter = new BrokerClientEvents(this);
    }

    private ManagedSocketBase<AsyncSocketHandle>? OnUpstreamConnected(ISocketManager<AsyncSocketHandle> manager, ConnectedEndPoints connection)
    {
        if (State != UpstreamState.NoConnection) return null;
        State = UpstreamState.WaitingForHandshake;
        UpstreamSocket = new WebSocketHandshake<AsyncSocketHandle>(manager, ServerAdapter);
        return UpstreamSocket;
    }

    private ManagedSocketBase<AsyncSocketHandle>? OnClientConnected(ISocketManager<AsyncSocketHandle> manager, ConnectedEndPoints connection)
    {
        if (State != UpstreamState.Connected) return null;
        return new TabServer<BrokerHandle, AsyncSocketHandle>(manager, ClientAdapter);
    }

    public Thread StartThread(AsyncSocketManager manager, EndPoint upstreamEndpoint, EndPoint clientEndpoint)
    {
        var thread = new Thread(StartThreadAdapter);
        thread.Start(new StartArgs()
        {
            Manager = manager,
            Upstream = upstreamEndpoint,
            Client = clientEndpoint,
        });
        return thread;
    }

    private void StartThreadAdapter(object? args)
    {
        if (args is StartArgs start)
        {
            Start(start.Manager, start.Upstream, start.Client);
        }
    }

    public void Start(AsyncSocketManager manager, EndPoint upstreamEndpoint, EndPoint clientEndpoint)
    {
        Manager = manager;
        Manager.Bind(upstreamEndpoint, OnUpstreamConnected);
        Manager.Bind(clientEndpoint, OnClientConnected);

        BrokerEvent? evt = null;
        while (true)
        {
            EventWaiter.WaitOne();
            if (!Events.TryDequeue(out evt)) continue;

            if (evt is StopBroker)
            {
                CloseAll();
                break;
            }
            else if (evt is UpstreamConnected upstreamConnected)
            {
                State = UpstreamState.Connected;
                UpstreamBroker = upstreamConnected.Handle;
            }
            else if (evt is UpstreamDisconnected)
            {
                State = UpstreamState.NoConnection;
                UpstreamSocket = null;
                foreach (var client in Clients.Values)
                {
                    client.Close();
                }
                Clients.Clear();
            }
            else if (evt is ClientConnected connected)
            {
                OnClientConnected(connected);
            }
            else if (evt is ClientDisconnected disconnected)
            {
                OnClientDisconnected(disconnected);
            }
            else if (evt is ForwardToClient toClient)
            {
                SendToClient(toClient);
            }
            else if (evt is ForwardToUpstream toUpstream)
            {
                SendToUpstream(toUpstream);
            }
        }
    }

    public void PostEvent(BrokerEvent evt)
    {
        Events.Enqueue(evt);
        EventWaiter.Set();
    }

    private void OnClientConnected(ClientConnected evt)
    {
        // Possible if the client sent the HELLO at the same time as the upstream was disconnecting.
        // In this case the client has already been closed during the UpstreamDisconnected event,
        // just make sure not to send the message to the upstream that no longer exists.
        if (State != UpstreamState.Connected) return;

        Clients[evt.Client.Id] = evt.Client;
        SendMessageToUpstream(new HelloBrokerCommand()
        {
            Id = evt.Client.Id,
            Name = evt.Name,
        });
    }

    private void OnClientDisconnected(ClientDisconnected evt)
    {
        if (State != UpstreamState.Connected) return;

        Clients[evt.Client.Id] = evt.Client;
        SendMessageToUpstream(new GoodbyeBrokerCommand()
        {
            Id = evt.Client.Id,
        });
    }

    private void SendToUpstream(ForwardToUpstream toUpstream)
    {
        if (State != UpstreamState.Connected) return;

        SendMessageToUpstream(new SendBrokerCommand()
        {
            Id = toUpstream.SourceClient.Id,
            Command = toUpstream.Message,
        });
    }

    private void SendToClient(ForwardToClient toClient)
    {
        BrokerHandle client;
        if (!Clients.TryGetValue(toClient.DestinationClient, out client)) return;

        var size = toClient.Message.Count;
        var buffer = client.PrepareSend(size);
        toClient.Message.CopyTo(buffer);
        client.Send(size);
    }

    private void SendMessageToUpstream(BrokerCommand command)
    {
        var size = command.EncodedSize();
        var buffer = UpstreamBroker.PrepareSend(size);
        command.EncodeTo(buffer);
        UpstreamBroker.Send(size);
    }

    private void CloseAll()
    {
        if (State == UpstreamState.WaitingForHandshake)
        {
            UpstreamSocket.Close();
        }
        else if (State == UpstreamState.Connected)
        {
            UpstreamBroker.Close();
        }

        foreach (var client in Clients.Values)
        {
            client.Close();
        }
    }
}

public class BrokerServerEvents : IBrokerServer
{
    private BrokerEventDispatcher Dispatcher;

    public BrokerServerEvents(BrokerEventDispatcher dispatcher)
    {
        Dispatcher = dispatcher;
    }

    public void UpstreamConnected(IBrokerConnection connection)
    {
        Dispatcher.PostEvent(new brokerlib.UpstreamConnected()
        {
            Handle = new BrokerHandle(connection),
        });
    }

    public void UpstreamDisconnected()
    {
        Dispatcher.PostEvent(brokerlib.UpstreamDisconnected.Instance);
    }

    public void ForwardToClient(int client, ArraySegment<byte> message)
    {
        Dispatcher.PostEvent(new ForwardToClient()
        {
            DestinationClient = client,
            Message = message,
        });
    }
}

public class BrokerClientEvents : IBrokerClient<BrokerHandle>
{
    private BrokerEventDispatcher Dispatcher;

    public BrokerClientEvents(BrokerEventDispatcher dispatcher)
    {
        Dispatcher = dispatcher;
    }

    public BrokerHandle RegisterClient(IBrokerConnection connection, ArraySegment<byte> name)
    {
        var client = new BrokerHandle(connection);
        Dispatcher.PostEvent(new ClientConnected()
        {
            Client = client,
            Name = name,
        });
        return client;
    }

    public void UnregisterClient(BrokerHandle client)
    {
        Dispatcher.PostEvent(new ClientDisconnected()
        {
            Client = client,
        });
    }

    public void ForwardToServer(BrokerHandle client, ArraySegment<byte> message)
    {
        Dispatcher.PostEvent(new ForwardToUpstream()
        {
            SourceClient = client,
            Message = message,
        });
    }
}
