// -*- mode: csharp; fill-column: 100 -*-
using System.Text;

namespace brokerlib.tests;

public class WebScoketServerTests : TestUtil
{
    [Test]
    public void AcceptMessage()
    {
        var manager = new DummyManager();
        var broker = new DummyServerBroker();
        var server = new WebSocketServer(manager, broker);

        var id = Guid.NewGuid().ToString();
        var message = "{\"id\": \"" + id + "\", \"line\": \"stuff and things\"}";
        var messageBytes = Encoding.UTF8.GetBytes(message);

        var request = new WebSocketFrame()
        {
            IsLastFragment = true,
            OpCode = MessageType.Text,
            Mask = FillRandomBytes(4),
            Payload = messageBytes.Length,
        };
        var requestBytes = request.ToArray(messageBytes);
        manager.EnqueueReceive(new Memory<byte>(requestBytes));
        // Never actually sent, just a backstop to ensure there is a message in
        // the queue when Receive is called
        manager.EnqueueReceive(new Memory<byte>(new byte[0]));

        var command = new EncodedCommand();
        command.Id = id;
        command.Command = "stuff and things";

        server.OnConnected();
        manager.Step();

        Assert.AreEqual(new byte[0], manager.RawOutput());
        Assert.AreEqual(1, broker.Commands.Count);
        Assert.AreEqual(command, broker.Commands[0]);
        Assert.IsTrue(broker.IsConnected);
    }

    [Test]
    public void SendReply()
    {
        var manager = new DummyManager();
        var broker = new DummyServerBroker();
        var server = new WebSocketServer(manager, broker);

        // Never actually sent, just a backstop to ensure there is a message in
        // the queue when Receive is called
        manager.EnqueueReceive(new Memory<byte>(new byte[0]));

        var id = Guid.NewGuid().ToString();
        var message = "{\"id\": \"" + id + "\", \"hello\": \"hi world\"}";
        var messageBytes = Encoding.UTF8.GetBytes(message);

        server.OnConnected();
        server.SendToUpstream(message);

        var request = new WebSocketFrame()
        {
            IsLastFragment = true,
            OpCode = MessageType.Text,
            Mask = null,
            Payload = messageBytes.Length,
        };
        var requestBytes = request.ToArray(messageBytes);

        Assert.AreEqual(requestBytes, manager.RawOutput());
        Assert.AreEqual(0, broker.Commands.Count);
        Assert.IsTrue(broker.IsConnected);
    }

    [Test]
    public void AcceptPing()
    {
        var manager = new DummyManager();
        var broker = new DummyServerBroker();
        var server = new WebSocketServer(manager, broker);

        var request = new WebSocketFrame()
        {
            IsLastFragment = true,
            OpCode = MessageType.Ping,
            Mask = FillRandomBytes(4),
            Payload = 100,
        };
        var messageBytes = FillRandomBytes(100);
        var requestBytes = request.ToArray(messageBytes);
        manager.EnqueueReceive(new Memory<byte>(requestBytes));
        // Never actually sent, just a backstop to ensure there is a message in
        // the queue when Receive is called
        manager.EnqueueReceive(new Memory<byte>(new byte[0]));

        var response = new WebSocketFrame()
        {
            IsLastFragment = true,
            OpCode = MessageType.Pong,
            Mask = null,
            Payload = request.Payload,
        };
        var responseBytes = response.ToArray(messageBytes);

        server.OnConnected();
        manager.Step();

        Assert.AreEqual(responseBytes, manager.RawOutput());
        Assert.AreEqual(0, broker.Commands.Count);
        Assert.IsTrue(broker.IsConnected);
    }

    [Test]
    public void AcceptClose()
    {
        var manager = new DummyManager();
        var broker = new DummyServerBroker();
        var server = new WebSocketServer(manager, broker);

        var request = new WebSocketFrame()
        {
            IsLastFragment = true,
            OpCode = MessageType.Close,
            Mask = FillRandomBytes(4),
            Payload = 100,
        };
        var requestBytes = request.ToArray(FillRandomBytes(100));
        manager.EnqueueReceive(new Memory<byte>(requestBytes));
        // Must *not* enqueue the dummy message here, we should never receive after a Close

        var response = new WebSocketFrame()
        {
            IsLastFragment = true,
            OpCode = MessageType.Close,
            Mask = null,
            Payload = 0,
        };
        var responseBytes = response.ToArray(null);

        server.OnConnected();
        manager.Step();

        Assert.AreEqual(responseBytes, manager.RawOutput());
        Assert.AreEqual(0, broker.Commands.Count);
        Assert.IsFalse(broker.IsConnected);
    }

    [Test]
    public void IgnoreBinary()
    {
        var manager = new DummyManager();
        var broker = new DummyServerBroker();
        var server = new WebSocketServer(manager, broker);

        var request = new WebSocketFrame()
        {
            IsLastFragment = true,
            OpCode = MessageType.Binary,
            Mask = FillRandomBytes(4),
            Payload = 100,
        };
        var requestBytes = request.ToArray(FillRandomBytes(100));
        manager.EnqueueReceive(new Memory<byte>(requestBytes));
        // Never actually sent, just a backstop to ensure there is a message in
        // the queue when Receive is called
        manager.EnqueueReceive(new Memory<byte>(new byte[0]));

        server.OnConnected();
        manager.Step();

        Assert.AreEqual(new byte[0], manager.RawOutput());
        Assert.AreEqual(0, broker.Commands.Count);
        Assert.IsTrue(broker.IsConnected);
    }

    [Test]
    public void IgnorePong()
    {
        var manager = new DummyManager();
        var broker = new DummyServerBroker();
        var server = new WebSocketServer(manager, broker);

        var request = new WebSocketFrame()
        {
            IsLastFragment = true,
            OpCode = MessageType.Pong,
            Mask = FillRandomBytes(4),
            Payload = 100,
        };
        var requestBytes = request.ToArray(FillRandomBytes(100));
        manager.EnqueueReceive(new Memory<byte>(requestBytes));
        // Never actually sent, just a backstop to ensure there is a message in
        // the queue when Receive is called
        manager.EnqueueReceive(new Memory<byte>(new byte[0]));

        server.OnConnected();
        manager.Step();

        Assert.AreEqual(new byte[0], manager.RawOutput());
        Assert.AreEqual(0, broker.Commands.Count);
        Assert.IsTrue(broker.IsConnected);
    }
}
