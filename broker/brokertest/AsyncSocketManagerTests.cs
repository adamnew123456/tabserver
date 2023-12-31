// -*- mode: csharp; fill-column: 100 -*-
using System.IO;
using System.Net;
using System.Net.Sockets;

class EchoSocket<SocketT> : ManagedSocketBase<SocketT>
{
    public bool IsClosed;
    private byte[] ReceiveBuffer = new byte[8192];

    public EchoSocket(ISocketManager<SocketT> manager)
    {
        Manager = manager;
        IsClosed = false;
    }

    override public void OnConnected()
    {
        Manager.Receive(ManagerHandle, new ArraySegment<byte>(ReceiveBuffer));
    }

    override public void OnReceive(ArraySegment<byte> destination)
    {
        Manager.SendAll(ManagerHandle, destination);
    }

    override public void OnSend()
    {
        Manager.Receive(ManagerHandle, new ArraySegment<byte>(ReceiveBuffer));
    }

    override public void OnClose()
    {
        IsClosed = true;
    }
}

public class AsyncSocketManagerTests : TestUtil
{
    private ManagedSocketBase<AsyncSocketHandle>? EchoFactory(ISocketManager<AsyncSocketHandle> manager, ConnectedEndPoints endpoints)
    {
        return new EchoSocket<AsyncSocketHandle>(manager);
    }

    private ManagedSocketBase<AsyncSocketHandle>? RejectFactory(ISocketManager<AsyncSocketHandle> manager, ConnectedEndPoints endpoints)
    {
        return null;
    }

    [Test]
    public void SocketLifecycle()
    {
        var sendBuffer = FillRandomBytes(10000);
        var receiveBuffer = new byte[10000];

        var manager = new AsyncSocketManager();
        try
        {
            var address = manager.Bind(new IPEndPoint(IPAddress.Loopback, 0), EchoFactory);

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

    [Test]
    public void RefuseConnection()
    {
        var manager = new AsyncSocketManager();
        try
        {
            var address = manager.Bind(new IPEndPoint(IPAddress.Loopback, 0), RejectFactory);

            var client = new Socket(SocketType.Stream, ProtocolType.Tcp);
            client.Connect(address);

            // It will connect but then the server will hang up, detect this by a receive
            byte[] buffer = new byte[1];
            var received = client.Receive(buffer);
            Assert.AreEqual(0, received);

            client.Close();
        }
        finally
        {
            manager.CloseAll();
        }
    }
}
