// -*- mode: csharp; fill-column: 100 -*-
using System.Net;
using System.Net.Sockets;
using System.Text.Json.Serialization;

namespace brokerlib;

public class EncodedCommand : IEquatable<EncodedCommand>
{
    [JsonPropertyName("id")]
    public string? Id { get; set; }

    [JsonPropertyName("line")]
    public string? Command { get; set; }

    public bool Equals(EncodedCommand? other)
    {
        if (other == null) return false;
        return this.Id == other.Id && other.Command == this.Command;
    }

    public override string ToString()
    {
        return $"EncodedCommand<from={Id}, message={Command}>";
    }
}

public class AsyncSocketHandle
{
    internal Socket Connection;
    internal SocketAsyncEventArgs Event;
    internal EventHandler<SocketAsyncEventArgs> Callback;
    internal IManagedSocket<AsyncSocketHandle> Socket;
    internal int SendBufferOffset;
    internal int SendBufferLength;

    internal AsyncSocketHandle(Socket connection,
                               IManagedSocket<AsyncSocketHandle> socket,
                               EventHandler<SocketAsyncEventArgs> callback)
    {
        Connection = connection;
        Socket = socket;
        Callback = callback;
        Event = new SocketAsyncEventArgs();

        Event.Completed += callback;
        Event.UserToken = this;
    }

    /// Invokes the server socket's callback when data is available.
    internal void ReceiveAsync(ArraySegment<byte> buffer)
    {
        Event.SetBuffer(buffer.Array, buffer.Offset, buffer.Count);

        var pending = Connection.ReceiveAsync(Event);
        if (!pending)
        {
            Callback(null, Event);
        }
    }

    /// Invokes the server socket's callback when data is available.
    internal void SendAsync(ArraySegment<byte> buffer)
    {
        SendBufferOffset = buffer.Offset;
        SendBufferLength = buffer.Count;
        Event.SetBuffer(buffer.Array, buffer.Offset, buffer.Count);

        var pending = Connection.SendAsync(Event);
        if (!pending)
        {
            Callback(null, Event);
        }
    }

    /// Checks if any more data must be sent from the send buffer and sends it asynchronously.
    /// Returns true if another send was triggered and false otherwise.
    internal bool ContinueSending()
    {
        SendBufferLength -= Event.BytesTransferred;
        if (SendBufferLength == 0) return false;

        SendBufferOffset += Event.BytesTransferred;
        SendAsync(new ArraySegment<byte>(Event.Buffer, SendBufferOffset, SendBufferLength));
        return true;
    }

    /// Closes the socket
    internal void Close()
    {
        Connection.Close();
        Socket.OnClose();
    }

    public void Dispose()
    {
        Event.Dispose();
        GC.SuppressFinalize(this);
    }
}

internal class AsyncServerSocket
{
    // NOTE: While the SocketAsyncEventArgs in this object are disposable, we deliberately do not
    // implement Dispose on the class because there's no situation where we want to clean this up
    // and continue doing other things. Client connections come and go but the server is bound for
    // the entire lifetime of the program.

    internal Socket Listener;
    internal SocketAsyncEventArgs Event;
    internal EventHandler<SocketAsyncEventArgs> Callback;
    internal ManagedSocketFactory<AsyncSocketHandle> Factory;

    internal AsyncServerSocket(Socket listener,
                               ManagedSocketFactory<AsyncSocketHandle> factory,
                               EventHandler<SocketAsyncEventArgs> callback)
    {
        Listener = listener;
        Event = new SocketAsyncEventArgs();
        Factory = factory;
        Callback = callback;

        Event.Completed += callback;
        Event.UserToken = this;
    }

    /// Invokes the server socket's callback when a client connects
    internal void AcceptAsync()
    {
        Event.AcceptSocket = null;
        var pending = Listener.AcceptAsync(Event);
        if (!pending)
        {
            Callback(null, Event);
        }
    }
}

public class AsyncSocketManager : ISocketManager<AsyncSocketHandle>
{
    private HashSet<Socket> ServerSockets = new HashSet<Socket>();
    private HashSet<Socket> ClientSockets = new HashSet<Socket>();

    /// Processes successful and failed async accept requests on any AsyncServerSocket
    private void ServerCallback(object? sender, SocketAsyncEventArgs evt)
    {
        var serverSock = (AsyncServerSocket) evt.UserToken;
        if (evt.SocketError != SocketError.Success)
        {
            string endpoint = null;
            try
            {
                endpoint = serverSock.Listener.LocalEndPoint.ToString();
            }
            catch (Exception err)
            {
                endpoint = "<unknown>";
            }
            Console.WriteLine($"[ACCEPT] Error: {endpoint} cannot accept connection, {evt.SocketError}");

            if (evt.SocketError == SocketError.OperationAborted) return;
        }
        else
        {
            var clientSock = evt.AcceptSocket;
            var endpoints = new ConnectedEndPoints(clientSock.LocalEndPoint, clientSock.RemoteEndPoint);
            var managedSock = serverSock.Factory(this, endpoints);
            if (managedSock == null)
            {
                Console.WriteLine($"[ACCEPT] Error: {endpoints} refused connection");
                clientSock.Close();
            }
            else
            {
                ClientSockets.Add(clientSock);
                var handle = new AsyncSocketHandle(clientSock, managedSock, ClientCallback);
                handle.Socket.ManagerHandle = handle;
                managedSock.OnConnected();
            }
        }

        serverSock.AcceptAsync();
    }

    private void ClientCallback(object? sender, SocketAsyncEventArgs evt)
    {
        var clientSock = (AsyncSocketHandle) evt.UserToken;
        if (evt.SocketError != SocketError.Success)
        {
            var localEndpoint = clientSock.Connection.LocalEndPoint;
            var remoteEndpoint = clientSock.Connection.RemoteEndPoint;
            Console.WriteLine($"[ACCEPT] Error: Aborting {localEndpoint} - {remoteEndpoint}, ${evt.SocketError}");
            clientSock.Close();
        }
        else if (evt.LastOperation == SocketAsyncOperation.Receive)
        {
            if (evt.BytesTransferred == 0)
            {
                var localEndpoint = clientSock.Connection.LocalEndPoint;
                var remoteEndpoint = clientSock.Connection.RemoteEndPoint;
                Console.WriteLine($"[ACCEPT] Error: Closing {localEndpoint} - {remoteEndpoint}, remote end hung up");
                clientSock.Close();
            }
            else
            {
                var segment = new ArraySegment<byte>(evt.Buffer, 0, evt.BytesTransferred);
                clientSock.Socket.OnReceive(segment);
            }
        }
        else if (evt.LastOperation == SocketAsyncOperation.Send)
        {
            if (!clientSock.ContinueSending())
            {
                clientSock.Socket.OnSend();
            }
        }
    }

    public EndPoint Bind(EndPoint address, ManagedSocketFactory<AsyncSocketHandle> factory)
    {
        var sock = new Socket(SocketType.Stream, ProtocolType.Tcp);
        sock.Bind(address);
        sock.Listen();
        var bound = sock.LocalEndPoint;
        ServerSockets.Add(sock);

        var serverSock = new AsyncServerSocket(sock, factory, ServerCallback);
        serverSock.AcceptAsync();
        return bound;
    }

    public void ChangeHandler(AsyncSocketHandle socket, IManagedSocket<AsyncSocketHandle> newSocket)
    {
        socket.Socket = newSocket;
        newSocket.ManagerHandle = socket;
        newSocket.OnConnected();
    }

    public void Receive(AsyncSocketHandle socket, ArraySegment<byte> destination)
    {
        socket.ReceiveAsync(destination);
    }

    public void SendAll(AsyncSocketHandle socket, ArraySegment<byte> source)
    {
        socket.SendAsync(source);
    }

    public void Close(AsyncSocketHandle socket)
    {
        socket.Close();
    }

    public void CloseAll()
    {
        foreach (var sock in ServerSockets)
        {
            sock.Close();
        }

        foreach (var sock in ClientSockets)
        {
            sock.Close();
        }
    }
}
