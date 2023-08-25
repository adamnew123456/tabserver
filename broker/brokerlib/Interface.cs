// -*- mode: csharp; fill-column: 100 -*-
using System.Net;

namespace brokerlib;

/// A pair of endpoints, indicating the local and remote ends of a connected socket.
public class ConnectedEndPoints
{
    /// The endpoint on the local host
    public EndPoint Local { get; }

    /// The endpoint on the remote host
    public EndPoint Remote { get; }

    public ConnectedEndPoints(EndPoint local, EndPoint remote)
    {
        Local = local;
        Remote = remote;
    }

    public override string ToString()
    {
        return $"<connection: ${Local} - ${Remote}>";
    }
}

/// Creates a socket wrapper when a client connects to the server. May return null to indicate that
/// the connection isn't acceptable and must be closed immediately.
public delegate IManagedSocket<SocketHandle>? ManagedSocketFactory<SocketHandle>(ISocketManager<SocketHandle> manager, ConnectedEndPoints connection);

/// Manages the async requests for each managed socket, configuring their event handlers and calling
/// their callbacks any time an event occurs.
public interface ISocketManager<SocketHandle>
{
    /// Binds a server socket and starts listening on it immediately. When a connection is received,
    /// the local and remote endpoint are given to a factory which returns a socket to use for the
    /// connection.
    ///
    /// Note that binds are permanent - once the socket is listening, there is no way to get it to
    /// stop. The only thing you can do is have the factory refuse connections on the socket. But
    /// the socket will still be listening.
    void Bind(EndPoint address, ManagedSocketFactory<SocketHandle> factory);

	/// Schedules an asynchronous receive into the destination buffer. Calls OnReceive when the data
	/// is available.
	void Receive(SocketHandle socket, ArraySegment<byte> destination);

	/// Schedules an asynchronous receive into the destination buffer. Calls OnSend when the data
	/// has been sent. Note that this may require multiple sends, if the buffer is to big to be sent
	/// in one shot.
	void SendAll(SocketHandle socket, ArraySegment<byte> source);

	/// Closes the socket immediately and executes OnClose.
	void Close(SocketHandle socket);

    /// Changes the socket that handles send and receive events and calls OnConnected on the new
    /// socket immediately after.
    ///
    /// Note that this also updates the callbacks of any in-flight receive or send events to use the
    /// new socket. Be sure that the new socket can cope with these new events, or otherwise make
    /// sure that there aren't any outstanding async requests.
    void ChangeHandler(SocketHandle socket, IManagedSocket<SocketHandle> newSocket);
}

public interface IManagedSocket<SocketHandle>
{
    /// The socket manager that the socket uses to request operations.
	ISocketManager<SocketHandle> Manager { get; }

    /// The handle assigned to this socket by the manager.
    SocketHandle ManagerHandle { set; }

	/// Called by the socket manager when the connection is opened.
	void OnConnected();

	/// Called by the socket manager when a Receive request completes. Note that the destination is
	/// sliced to fit the amount of data that is actually received. However, this destination has
	/// the same backing array as the one given to receive and no copy is performed.
	void OnReceive(ArraySegment<byte> destination);

	/// Called by the socket manager when a SendAll request completes.
	void OnSend();

	/// Called by the socket manager when a Close request completes, or the socket is forcibly
	/// closed by the peer or the operating system.
	void OnClose();
}

/// The portion of the broker that's exposed to TabServer client connections. This end of the broker
/// can only be used to post messages received by the TabServer clients up to the upstream server.
public interface IBrokerClient<ClientHandle>
{
    /// Registers a new client, returning a handle that identifies the client to the upstream
    /// server.
    ClientHandle RegisterClient(string name);

    /// Unregisters a client
    void UnregisterClient(ClientHandle client);

    /// Forwards a fragment of a message from the indicated client to the upstream server. flush is
    /// used to control whether the message is complete: if it's false then the fragment is added to
    /// the broker's buffer for this client, if it's true then the fragment (and any buffered data)
    /// are sent to the upstream.
    void ForwardToServer(ClientHandle client, string message, bool flush);
}

/// The portion of the broker that's exposed to the upstream server. This end is used to notify the
/// broker when the usptream connection dies, and to pass along any replies from the upstream server
/// to individual clients.
public interface IBrokerServer
{
    /// Indicates that the connection to the upstream server has been established. Client
    /// connections are now allowed.
    void UpstreamConnected();

	/// Indicates that the connection to the upstream server has been lost. Must terminate all
	/// active client connections.
	void UpstreamDisconnected();

    /// Processes a message from the upstream server
    void ProcessMessage(EncodedCommand command);
}

/// A broker must be able to send messages over each connection, both from the clients to the
/// upstream, and the upstreams to the client. The contents of each message depend on the connection
/// type:
///
/// - Client connections contain a line of ASCII text, including the trailing newline.
/// - Server connections contain UTF-8 encoded JSON.
public interface IBrokerConnection
{
    /// Determines how big an array should be rented from the shared ArrayPool for a message of the
    /// given size (in bytes). Returns the total size of the buffer required.
    public int MessageCapacity(int messageBytes);

    /// Sends a message over the client's connection. The array is rented from the shared ArrayPool
    /// using the size specified in MessageCapacity, it is up to the connection to return it once
    /// the send has completed.
    ///
    /// If the capacity is larger than the message size, the message is stored after the extra
    /// bytes. For example, if MessageCapacity(6) == 10 then the message is stored from bytes 4
    /// through 9 and bytes 0 through 3 are left empty.
    public void SendMessage(ArraySegment<byte> buffer, int messageBytes);
}
