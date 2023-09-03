// -*- mode: csharp; fill-column: 100 -*-
using System.Text;

namespace brokerlib.tests;

public class TabServerTests : TestUtil
{
    [SetUp]
    public void SetUp()
    {
        DummyClientHandle.ResetIdGenerator();
    }

    [Test]
    public void AcceptHello()
    {
        var manager = new DummyManager();
        var broker = new DummyClientBroker();
        var connection = new TabServer<DummyClientHandle, DummySocketHandle>(manager, broker);
        manager.DirectBind(connection);

        var message1 = Encoding.ASCII.GetBytes("HELLO");
        var message2 = Encoding.ASCII.GetBytes("\n");
        var message3 = Encoding.ASCII.GetBytes("test client");
        var message4 = Encoding.ASCII.GetBytes("\n");
        manager.EnqueueReceive(new Memory<byte>(message1));
        manager.EnqueueReceive(new Memory<byte>(message2));
        manager.EnqueueReceive(new Memory<byte>(message3));
        manager.EnqueueReceive(new Memory<byte>(message4));
        manager.EnqueueEmpty();

        connection.OnConnected();
        Assert.AreEqual(0, broker.RegisteredClients.Count);

        manager.Step();
        Assert.AreEqual(0, broker.RegisteredClients.Count); // Haven't gotten HELLO or the name yet

        manager.Step();
        Assert.AreEqual(0, broker.RegisteredClients.Count); // Haven't gotten the name yet

        manager.Step();
        Assert.AreEqual(0, broker.RegisteredClients.Count); // Still no name...

        manager.Step();
        Assert.AreEqual(1, broker.RegisteredClients.Count);
        Assert.AreEqual("0", broker.RegisteredClients.First().Id);

        connection.OnClose();
        Assert.AreEqual(0, broker.RegisteredClients.Count);
    }

    [Test]
    public void AcceptHelloBlock()
    {
        var manager = new DummyManager();
        var broker = new DummyClientBroker();
        var connection = new TabServer<DummyClientHandle, DummySocketHandle>(manager, broker);
        manager.DirectBind(connection);

        var message = Encoding.ASCII.GetBytes("HELLO\ntest client\n");
        manager.EnqueueReceive(new Memory<byte>(message));
        manager.EnqueueEmpty();

        connection.OnConnected();

        manager.Step();
        Assert.AreEqual(1, broker.RegisteredClients.Count);
        Assert.AreEqual("0", broker.RegisteredClients.First().Id);

        connection.OnClose();
        Assert.AreEqual(0, broker.RegisteredClients.Count);
    }

    [Test]
    public void AcceptHelloPerByte()
    {
        var manager = new DummyManager();
        var broker = new DummyClientBroker();
        var connection = new TabServer<DummyClientHandle, DummySocketHandle>(manager, broker);
        manager.DirectBind(connection);

        var message = Encoding.ASCII.GetBytes("HELLO\ntest client\n");
        for (var i = 0; i < message.Length; i++)
        {
            manager.EnqueueReceive(new Memory<byte>(message, i, 1));
        }
        manager.EnqueueEmpty();

        connection.OnConnected();
        for (var i = 0; i < message.Length; i++)
        {
            manager.Step();
            if (i == message.Length - 1)
            {
                Assert.AreEqual(1, broker.RegisteredClients.Count);
                Assert.AreEqual("0", broker.RegisteredClients.First().Id);
            }
            else
            {
                Assert.AreEqual(0, broker.RegisteredClients.Count);
            }
        }

        connection.OnClose();
        Assert.AreEqual(0, broker.RegisteredClients.Count);
    }

    [Test]
    public void AcceptData()
    {
        var manager = new DummyManager();
        var broker = new DummyClientBroker();
        var connection = new TabServer<DummyClientHandle, DummySocketHandle>(manager, broker);
        manager.DirectBind(connection);

        var message = Encoding.ASCII.GetBytes("HELLO\ntest client\nmessage 1\nmessage 2\nmessage 3\n");
        manager.EnqueueReceive(message);
        manager.EnqueueEmpty();

        connection.OnConnected();
        manager.Step();

        Assert.AreEqual(1, broker.RegisteredClients.Count);
        var client = broker.RegisteredClients.First();
        Assert.AreEqual(1, client.Fragments.Count);
        Assert.AreEqual("message 1\nmessage 2\nmessage 3\n", client.Fragments[0]);

        connection.OnClose();
        Assert.AreEqual(0, broker.RegisteredClients.Count);
    }

    [Test]
    public void AcceptDataFragmented()
    {
        var manager = new DummyManager();
        var broker = new DummyClientBroker();
        var connection = new TabServer<DummyClientHandle, DummySocketHandle>(manager, broker);
        manager.DirectBind(connection);

        var message1 = Encoding.ASCII.GetBytes("HELLO\ntest client\n");
        var message2 = Encoding.ASCII.GetBytes("mess");
        var message3 = Encoding.ASCII.GetBytes("age 1");
        var message4 = Encoding.ASCII.GetBytes("\nmes");
        var message5 = Encoding.ASCII.GetBytes("sage");
        var message6 = Encoding.ASCII.GetBytes(" 2\nmessage 3\n");
        manager.EnqueueReceive(message1);
        manager.EnqueueReceive(message2);
        manager.EnqueueReceive(message3);
        manager.EnqueueReceive(message4);
        manager.EnqueueReceive(message5);
        manager.EnqueueReceive(message6);
        manager.EnqueueEmpty();

        connection.OnConnected();
        manager.Step();

        Assert.AreEqual(1, broker.RegisteredClients.Count);

        for (var i = 0; i < 5; i++) manager.Step();

        var client = broker.RegisteredClients.First();
        Assert.AreEqual(5, client.Fragments.Count);
        Assert.AreEqual("mess", client.Fragments[0]);
        Assert.AreEqual("age 1", client.Fragments[1]);
        Assert.AreEqual("\nmes", client.Fragments[2]);
        Assert.AreEqual("sage", client.Fragments[3]);
        Assert.AreEqual(" 2\nmessage 3\n", client.Fragments[4]);

        connection.OnClose();
        Assert.AreEqual(0, broker.RegisteredClients.Count);
    }
}
