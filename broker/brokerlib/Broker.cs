// -*- mode: csharp; fill-column: 100 -*-
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

/// Manages the async requests for each managed socket, configuring their event handlers and calling
/// their callbacks any time an event occurs.
public interface ISocketManager
{
	/// Schedules an asynchronous receive into the destination buffer. Calls OnReceive when the data
	/// is available.
	void Receive(IManagedSocket socket, Memory<byte> destination);

	/// Schedules an asynchronous receive into the destination buffer. Calls OnSend when the data
	/// has been sent. Note that this may require multiple sends, if the buffer is to big to be sent
	/// in one shot.
	void SendAll(IManagedSocket socket, Memory<byte> source);

	/// Closes the socket immediately and executes OnClose.
	void Close(IManagedSocket socket);
}

public interface IManagedSocket
{
	ISocketManager Manager { get; }

	/// Called by the socket manager when the connection is opened.
	void OnConnected();

	/// Called by the socket manager when a Receive request completes. Note that the destination is
	/// sliced to fit the amount of data that is actually received. However, this destination has
	/// the same backing array as the one given to receive and no copy is performed.
	void OnReceive(Memory<byte> destination);

	/// Called by the socket manager when a SendAll request completes.
	void OnSend();

	/// Called by the socket manager when a Close request completes, or the socket is forcibly
	/// closed by the peer or the operating system.
	void OnClose();
}

/// The portion of the broker that's exposed to TabServer client connections. This end of the broker
/// can only be used to post messages received by the TabServer clients up to the upstream server.
public interface IBrokerClient
{
    /// Registers a new client, returning the GUID that identifies the client to the upstream server.
    string RegisterClient(string name);

    /// Unregisters a client
    void UnregisterClient(string name);

    /// Forwards a message from the indicated client to the upstream server.
    void ForwardToServer(string source, string line);
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
