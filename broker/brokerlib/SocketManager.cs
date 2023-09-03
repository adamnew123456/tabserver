// -*- mode: csharp; fill-column: 100 -*-
using System.Net;
using System.Net.Sockets;

namespace brokerlib;

public class AsyncSocketHandle
{
    internal Socket Connection;
    internal SocketAsyncEventArgs SendEvent;
    internal SocketAsyncEventArgs ReceiveEvent;
    internal EventHandler<SocketAsyncEventArgs> Callback;
    internal ManagedSocketBase<AsyncSocketHandle> Socket;
    internal int SendBufferOffset;
    internal int SendBufferLength;

    internal AsyncSocketHandle(Socket connection,
                               ManagedSocketBase<AsyncSocketHandle> socket,
                               EventHandler<SocketAsyncEventArgs> callback)
    {
        Connection = connection;
        Socket = socket;
        Callback = callback;
        SendEvent = new SocketAsyncEventArgs();
        ReceiveEvent = new SocketAsyncEventArgs();

        SendEvent.Completed += callback;
        ReceiveEvent.Completed += callback;
        SendEvent.UserToken = this;
        ReceiveEvent.UserToken = this;
    }

    /// Invokes the manager's callback when data is available.
    internal void ReceiveAsync(ArraySegment<byte> buffer)
    {
        ReceiveEvent.SetBuffer(buffer.Array, buffer.Offset, buffer.Count);

        var pending = Connection.ReceiveAsync(ReceiveEvent);
        if (!pending)
        {
            Callback(null, ReceiveEvent);
        }
    }

    /// Sends data and invokes the manager's callback when the send has completed.
    internal void SendAsync(ArraySegment<byte> buffer)
    {
        SendBufferOffset = buffer.Offset;
        SendBufferLength = buffer.Count;
        SendEvent.SetBuffer(buffer.Array, buffer.Offset, buffer.Count);

        var pending = Connection.SendAsync(SendEvent);
        if (!pending)
        {
            Callback(null, SendEvent);
        }
    }

    /// Checks if any more data must be sent from the send buffer and sends it asynchronously.
    /// Returns true if another send was triggered and false otherwise.
    internal bool ContinueSending()
    {
        SendBufferLength -= SendEvent.BytesTransferred;
        if (SendBufferLength == 0) return false;

        SendBufferOffset += SendEvent.BytesTransferred;
        SendAsync(new ArraySegment<byte>(SendEvent.Buffer, SendBufferOffset, SendBufferLength));
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
        SendEvent.Dispose();
        ReceiveEvent.Dispose();
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
                lock (this)
                {
                    ClientSockets.Add(clientSock);
                }
                Console.WriteLine($"[ACCEPT] Initiating protocol {managedSock} on {endpoints}");
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
            try
            {
                var localEndpoint = clientSock.Connection.LocalEndPoint;
                var remoteEndpoint = clientSock.Connection.RemoteEndPoint;
                Console.WriteLine($"[ACCEPT] Error: Aborting {localEndpoint} - {remoteEndpoint}, ${evt.SocketError}");
            }
            catch (ObjectDisposedException)
            {
                // The socket must have died before we got to this point, just don't log its details
                Console.WriteLine($"[ACCEPT] Error: Aborting dead socket ${evt.SocketError}");
            }

            lock (this)
            {
                ClientSockets.Remove(clientSock.Connection);
            }
            clientSock.Close();
        }
        else if (evt.LastOperation == SocketAsyncOperation.Receive)
        {
            if (evt.BytesTransferred == 0)
            {
                var localEndpoint = clientSock.Connection.LocalEndPoint;
                var remoteEndpoint = clientSock.Connection.RemoteEndPoint;
                Console.WriteLine($"[ACCEPT] Error: Closing {localEndpoint} - {remoteEndpoint}, remote end hung up");
                lock (this)
                {
                    ClientSockets.Remove(clientSock.Connection);
                }
                clientSock.Close();
                clientSock.Dispose();
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

    public void ChangeHandler(AsyncSocketHandle socket, ManagedSocketBase<AsyncSocketHandle> newSocket)
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
