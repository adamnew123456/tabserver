namespace brokerlib.tests;

using System.Text;

public class WebSocketFrameTests : TestUtil
{
    [Test]
	public void EmptyContinuation()
	{
		var frame = new WebSocketFrame()
		{
			IsLastFragment = true,
			OpCode = MessageType.Continuation,
			Mask = null,
			Payload = null
		};

		Assert.AreEqual(new byte[] {
			128, 0
		}, frame.ToArray());
	}

	[Test]
	public void EmptyText()
	{
		var frame = new WebSocketFrame()
		{
			IsLastFragment = true,
			OpCode = MessageType.Text,
			Mask = null,
			Payload = null
		};

		Assert.AreEqual(new byte[] {
			129, 0
		}, frame.ToArray());
	}

	[Test]
	public void EmptyBinary()
	{
		var frame = new WebSocketFrame()
		{
			IsLastFragment = true,
			OpCode = MessageType.Binary,
			Mask = null,
			Payload = null
		};

		Assert.AreEqual(new byte[] {
			130, 0
		}, frame.ToArray());
	}

	[Test]
	public void EmptyClose()
	{
		var frame = new WebSocketFrame()
		{
			IsLastFragment = true,
			OpCode = MessageType.Close,
			Mask = null,
			Payload = null
		};

		Assert.AreEqual(new byte[] {
			136, 0
		}, frame.ToArray());
	}

	[Test]
	public void EmptyPing()
	{
		var frame = new WebSocketFrame()
		{
			IsLastFragment = true,
			OpCode = MessageType.Ping,
			Mask = null,
			Payload = null
		};

		Assert.AreEqual(new byte[] {
			137, 0
		}, frame.ToArray());
	}

	[Test]
	public void EmptyPong()
	{
		var frame = new WebSocketFrame()
		{
			IsLastFragment = true,
			OpCode = MessageType.Pong,
			Mask = null,
			Payload = null
		};

		Assert.AreEqual(new byte[] {
			138, 0
		}, frame.ToArray());
	}

	[Test]
	public void Fragmented()
	{
		var frame = new WebSocketFrame()
		{
			IsLastFragment = false,
			OpCode = MessageType.Text,
			Mask = null,
			Payload = null
		};

		Assert.AreEqual(new byte[] {
			1, 0
		}, frame.ToArray());
	}

	[Test]
	public void ShortBody()
	{
		var body = FillRandomBytes(125);
		var frame = new WebSocketFrame()
		{
			IsLastFragment = true,
			OpCode = MessageType.Text,
			Mask = null,
			Payload = body,
		};

		Assert.AreEqual(new byte[] {129, 125}.Concat(body).ToArray(),
						frame.ToArray());
	}

	[Test]
	public void ShortBody126()
	{
		var body = FillRandomBytes(126);
		var frame = new WebSocketFrame()
		{
			IsLastFragment = true,
			OpCode = MessageType.Text,
			Mask = null,
			Payload = body,
		};

		Assert.AreEqual(new byte[] {129, 126, 0, 126}.Concat(body).ToArray(),
						frame.ToArray());
	}

	[Test]
	public void ShortBody127()
	{
		var body = FillRandomBytes(127);
		var frame = new WebSocketFrame()
		{
			IsLastFragment = true,
			OpCode = MessageType.Text,
			Mask = null,
			Payload = body,
		};

		Assert.AreEqual(new byte[] {129, 126, 0, 127}.Concat(body).ToArray(),
						frame.ToArray());
	}

	[Test]
	public void MidBody()
	{
		var body = FillRandomBytes(UInt16.MaxValue);
		var frame = new WebSocketFrame()
		{
			IsLastFragment = true,
			OpCode = MessageType.Text,
			Mask = null,
			Payload = body,
		};

		Assert.AreEqual(new byte[] {129, 126, 255, 255}.Concat(body).ToArray(),
						frame.ToArray());
	}

	[Test]
	public void LongBody()
	{
		var body = FillRandomBytes(UInt16.MaxValue + 1);
		var frame = new WebSocketFrame()
		{
			IsLastFragment = true,
			OpCode = MessageType.Text,
			Mask = null,
			Payload = body,
		};

		Assert.AreEqual(new byte[] {129, 127, 0, 0, 0, 0, 0, 1, 0, 0}.Concat(body).ToArray(),
						frame.ToArray());
	}

	[Test]
	public void Mask0()
	{
		var frame = new WebSocketFrame()
		{
			IsLastFragment = true,
			OpCode = MessageType.Text,
			Mask = new byte[] { 10, 20, 30, 40 },
			Payload = null,
		};

		Assert.AreEqual(new byte[] {129, 128, 10, 20, 30, 40 },
						frame.ToArray());
	}

	[Test]
	public void Mask1()
	{
		var frame = new WebSocketFrame()
		{
			IsLastFragment = true,
			OpCode = MessageType.Text,
			Mask = new byte[] { 10, 20, 30, 40 },
			Payload = new byte[] { 12 },
		};

		Assert.AreEqual(new byte[] {129, 129, 10, 20, 30, 40, 6},
						frame.ToArray());
	}

	[Test]
	public void Mask2()
	{
		var frame = new WebSocketFrame()
		{
			IsLastFragment = true,
			OpCode = MessageType.Text,
			Mask = new byte[] { 10, 20, 30, 40 },
			Payload = new byte[] { 12, 14 },
		};

		Assert.AreEqual(new byte[] {129, 130, 10, 20, 30, 40, 6, 26},
						frame.ToArray());
	}

	[Test]
	public void Mask3()
	{
		var frame = new WebSocketFrame()
		{
			IsLastFragment = true,
			OpCode = MessageType.Text,
			Mask = new byte[] { 10, 20, 30, 40 },
			Payload = new byte[] { 12, 14, 16 },
		};

		Assert.AreEqual(new byte[] {129, 131, 10, 20, 30, 40, 6, 26, 14},
						frame.ToArray());
	}


	[Test]
	public void Mask4()
	{
		var frame = new WebSocketFrame()
		{
			IsLastFragment = true,
			OpCode = MessageType.Text,
			Mask = new byte[] { 10, 20, 30, 40 },
			Payload = new byte[] { 12, 14, 16, 8 },
		};

		Assert.AreEqual(new byte[] {129, 132, 10, 20, 30, 40, 6, 26, 14, 32},
						frame.ToArray());
	}

	[Test]
	public void Mask5()
	{
		var frame = new WebSocketFrame()
		{
			IsLastFragment = true,
			OpCode = MessageType.Text,
			Mask = new byte[] { 10, 20, 30, 40 },
			Payload = new byte[] { 12, 14, 16, 8, 10 },
		};

		Assert.AreEqual(new byte[] {129, 133, 10, 20, 30, 40, 6, 26, 14, 32, 0},
						frame.ToArray());
	}
}
