// -*- mode: csharp; fill-column: 100 -*-
using System.Text;

namespace brokerlib.tests;

public class WebSocketHandshakeTests : TestUtil
{
    [Test]
    public void MinimalValidConnection()
    {
        var manager = new DummyManager();
        var handshake = new WebSocketHandshake(manager);

        var request =
            "GET / HTTP/1.1\r\n" +
            "Host: localhost:1234\r\n" +
            "Upgrade: websocket\r\n" +
            "Connection: Upgrade\r\n" +
            "Sec-WebSocket-Key: AAAAAAAAAAAAAAAAAAAAAA==\r\n" +
            "Sec-WebSocket-Version: 13\r\n\r\n";
        var srcBuffer = Encoding.UTF8.GetBytes(request);
        foreach (var chunk in ChunkBuffer(srcBuffer, 2048))
        {
            manager.EnqueueReceive(chunk);
        }

        handshake.OnConnected();
        while (handshake.Status == WebSocketHandshakeStatus.Pending) manager.Step();

        Assert.AreEqual(WebSocketHandshakeStatus.Successful, handshake.Status);
        Assert.IsTrue(manager.AllDataReceived());

        var expectedReply =
            "HTTP/1.1 101 Switching Protocols\r\n" +
            "Upgrade: websocket\r\n" +
            "Connection: Upgrade\r\n" +
            "Sec-WebSocket-Accept: ICX+Yqv66kxgM0FcWaLWlFLwTAI=\r\n\r\n";
        Assert.AreEqual(expectedReply, manager.DecodeOutput());
    }

    [Test]
    public void MinimalValidConnectionOneByOne()
    {
        var manager = new DummyManager();
        var handshake = new WebSocketHandshake(manager);

        var request =
            "GET / HTTP/1.1\r\n" +
            "Host: localhost:2345\r\n" +
            "Upgrade: websocket\r\n" +
            "Connection: Upgrade\r\n" +
            "Sec-WebSocket-Key: AAAAAAAAAAAAAAAAAAAAAA==\r\n" +
            "Sec-WebSocket-Version: 13\r\n\r\n";
        var srcBuffer = Encoding.UTF8.GetBytes(request);
        foreach (var chunk in ChunkBuffer(srcBuffer, 1))
        {
            manager.EnqueueReceive(chunk);
        }

        handshake.OnConnected();
        while (handshake.Status == WebSocketHandshakeStatus.Pending) manager.Step();

        Assert.AreEqual(WebSocketHandshakeStatus.Successful, handshake.Status);
        Assert.IsTrue(manager.AllDataReceived());

        var expectedReply =
            "HTTP/1.1 101 Switching Protocols\r\n" +
            "Upgrade: websocket\r\n" +
            "Connection: Upgrade\r\n" +
            "Sec-WebSocket-Accept: ICX+Yqv66kxgM0FcWaLWlFLwTAI=\r\n\r\n";
        Assert.AreEqual(expectedReply, manager.DecodeOutput());
    }

    [Test]
    public void ConnectionWithExtraHeaders()
    {
        var manager = new DummyManager();
        var handshake = new WebSocketHandshake(manager);

        var request =
            "GET / HTTP/1.1\r\n" +
            "Accept: */*\r\n" +
            "Accept-Encoding: gzip, deflate, br\r\n" +
            "Accept-Language: en-US,en;q=0.5\r\n" +
            "Cache-Control: no-cache\r\n" +
            "Connection: keep-alive, Upgrade\r\n" +
            "Host: libwebsockets.org\r\n" +
            "Origin: https://libwebsockets.org\r\n" +
            "Pragma: no-cache\r\n" +
            "Sec-Fetch-Dest: empty\r\n" +
            "Sec-Fetch-Mode: websocket\r\n" +
            "Sec-Fetch-Site: same-origin\r\n" +
            "Sec-WebSocket-Extensions: permessage-deflate\r\n" +
            "Sec-WebSocket-Key: sLmucQNIWOt6BmLkzRGqyg==\r\n" +
            "Sec-WebSocket-Protocol: dumb-increment-protocol\r\n" +
            "Sec-WebSocket-Version: 13\r\n" +
            "Upgrade: websocket\r\n" +
            "User-Agent: Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/118.0\r\n\r\n";
        var srcBuffer = Encoding.UTF8.GetBytes(request);
        foreach (var chunk in ChunkBuffer(srcBuffer, 2048))
        {
            manager.EnqueueReceive(chunk);
        }

        handshake.OnConnected();
        while (handshake.Status == WebSocketHandshakeStatus.Pending) manager.Step();

        Assert.AreEqual(WebSocketHandshakeStatus.Successful, handshake.Status);
        Assert.IsTrue(manager.AllDataReceived());

        var expectedReply =
            "HTTP/1.1 101 Switching Protocols\r\n" +
            "Upgrade: websocket\r\n" +
            "Connection: Upgrade\r\n" +
            "Sec-WebSocket-Accept: 29fdBKgC6B8m4rA82SjmA0K/cyQ=\r\n\r\n";
        Assert.AreEqual(expectedReply, manager.DecodeOutput());
    }
}
