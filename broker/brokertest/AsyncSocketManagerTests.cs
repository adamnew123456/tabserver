// -*- mode: csharp; fill-column: 100 -*-
using System.IO;
using System.Net;
using System.Net.Sockets;

class EchoSocket<SocketT> : IManagedSocket<SocketT>
{
    public bool IsClosed;

    public ISocketManager<SocketT> Manager { get; private set; }
    public SocketT ManagerHandle { private get; set; }

    private byte[] ReceiveBuffer = new byte[8192];

    public EchoSocket(ISocketManager<SocketT> manager)
    {
        Manager = manager;
        IsClosed = false;
    }

    public void OnConnected()
    {
        Manager.Receive(ManagerHandle, new ArraySegment<byte>(ReceiveBuffer));
    }

    public void OnReceive(ArraySegment<byte> destination)
    {
        Manager.SendAll(ManagerHandle, destination);
    }

    public void OnSend()
    {
        Manager.Receive(ManagerHandle, new ArraySegment<byte>(ReceiveBuffer));
    }

    public void OnClose()
    {
        IsClosed = true;
    }
}

public class AsyncSocketManagerTests : TestUtil
{
    private IManagedSocket<AsyncSocketHandle> Factory(ISocketManager<AsyncSocketHandle> manager, ConnectedEndPoints endpoints)
    {
        return new EchoSocket<AsyncSocketHandle>(manager);
    }

    private void Trace(string message)
    {
        File.AppendAllText("/dev/tty", message + "\n");
    }

    [Test]
    public void SocketLifecycle()
    {
        var sendBuffer = FillRandomBytes(10000);
        var receiveBuffer = new byte[10000];

        var manager = new AsyncSocketManager();
        try
        {
            var address = manager.Bind(new IPEndPoint(IPAddress.Loopback, 0), Factory);

            var client = new Socket(SocketType.Stream, ProtocolType.Tcp);
            client.NoDelay = true;
            client.Connect(address);

            var received = 0;
            var sent = 0;
            while (sent < sendBuffer.Length)
            {
                sent += client.Send(new Span<byte>(sendBuffer, sent, sendBuffer.Length - sent));
                received += client.Receive(new Span<byte>(receiveBuffer, received, receiveBuffer.Length - received));
            }

            while (received < receiveBuffer.Length)
            {
                received += client.Receive(new Span<byte>(receiveBuffer, received, receiveBuffer.Length - received));
            }

            client.Close();

            Assert.AreEqual(sent, received);
            Assert.AreEqual(sendBuffer, receiveBuffer);
        }
        finally
        {
            manager.CloseAll();
        }
    }
}
