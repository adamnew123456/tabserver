namespace brokerlib.tests;

using System.Text;

public class WebSocketClientTests : TestUtil
{
	private class ParserCallback
	{
		public List<WebSocketClientFrame> Frames = new List<WebSocketClientFrame>();

		public void Callback(WebSocketClientFrame frame)
		{
			Frames.Add(frame.ToOwned());
		}
	}

    private void SendFrameToParser(WebSocketClientParser parser, WebSocketFrame frame, Action<WebSocketClientFrame> callback, int chunkSize = -1)
	{
		SendArrayToParser(parser, frame.ToArray(), callback, chunkSize);
	}

    private void SendArrayToParser(WebSocketClientParser parser, byte[] buffer, Action<WebSocketClientFrame> callback, int chunkSize = -1)
	{
		var feedBuffer = parser.RentFeedBuffer();
		var cursor = new Memory<byte>(buffer);
		if (chunkSize <= 0) chunkSize = feedBuffer.Length;

		var offset = 0;
		var remaining = buffer.Length;
		while (remaining > 0)
		{
			var toCopy = Math.Min(chunkSize, remaining);
			cursor.Slice(offset, toCopy).CopyTo(feedBuffer);
			parser.Feed(toCopy, callback);
			offset += toCopy;
			remaining -= toCopy;
		}
	}

	[Test]
	public void ParseEmptyTextOneShot()
	{
		using (var parser = new WebSocketClientParser())
		{
			var cb = new ParserCallback();
			SendFrameToParser(parser,
				new WebSocketFrame()
				{
					IsLastFragment = true,
					OpCode = MessageType.Text,
					Mask = new byte[] { 10, 20, 30, 40 },
					Payload = null,
				},
				cb.Callback);

            var expected = new WebSocketClientFrame(MessageType.Text, new byte[0]);
			Assert.AreEqual(1, cb.Frames.Count);
			Assert.AreEqual(expected, cb.Frames[0]);
		}
	}

	[Test]
	public void ParseEmptyTextPerByte()
	{
		using (var parser = new WebSocketClientParser())
		{
			var cb = new ParserCallback();
			SendFrameToParser(parser,
				new WebSocketFrame()
				{
					IsLastFragment = true,
					OpCode = MessageType.Text,
					Mask = new byte[] { 10, 20, 30, 40 },
					Payload = null,
				},
				cb.Callback);

            var expected = new WebSocketClientFrame(MessageType.Text, new byte[0]);
			Assert.AreEqual(1, cb.Frames.Count);
			Assert.AreEqual(expected, cb.Frames[0]);
		}
	}

	[Test]
	public void ParseEmptyBinary()
	{
		using (var parser = new WebSocketClientParser())
		{
			var cb = new ParserCallback();
			SendFrameToParser(parser,
				new WebSocketFrame()
				{
					IsLastFragment = true,
					OpCode = MessageType.Binary,
					Mask = new byte[] { 10, 20, 30, 40 },
					Payload = null,
				},
				cb.Callback);

            var expected = new WebSocketClientFrame(MessageType.Binary, new byte[0]);
			Assert.AreEqual(1, cb.Frames.Count);
			Assert.AreEqual(expected, cb.Frames[0]);
		}
	}

	[Test]
	public void ParseEmptyClose()
	{
		using (var parser = new WebSocketClientParser())
		{
			var cb = new ParserCallback();
			SendFrameToParser(parser,
				new WebSocketFrame()
				{
					IsLastFragment = true,
					OpCode = MessageType.Close,
					Mask = new byte[] { 10, 20, 30, 40 },
					Payload = null,
				},
				cb.Callback);

            var expected = new WebSocketClientFrame(MessageType.Close, new byte[0]);
			Assert.AreEqual(1, cb.Frames.Count);
			Assert.AreEqual(expected, cb.Frames[0]);
		}
	}

	[Test]
	public void ParseEmptyPing()
	{
		using (var parser = new WebSocketClientParser())
		{
			var cb = new ParserCallback();
			SendFrameToParser(parser,
				new WebSocketFrame()
				{
					IsLastFragment = true,
					OpCode = MessageType.Ping,
					Mask = new byte[] { 10, 20, 30, 40 },
					Payload = null,
				},
				cb.Callback);

            var expected = new WebSocketClientFrame(MessageType.Ping, new byte[0]);
			Assert.AreEqual(1, cb.Frames.Count);
			Assert.AreEqual(expected, cb.Frames[0]);
		}
	}

	[Test]
	public void ParseEmptyPong()
	{
		using (var parser = new WebSocketClientParser())
		{
			var cb = new ParserCallback();
			SendFrameToParser(parser,
				new WebSocketFrame()
				{
					IsLastFragment = true,
					OpCode = MessageType.Pong,
					Mask = new byte[] { 10, 20, 30, 40 },
					Payload = null,
				},
				cb.Callback);

            var expected = new WebSocketClientFrame(MessageType.Pong, new byte[0]);
			Assert.AreEqual(1, cb.Frames.Count);
			Assert.AreEqual(expected, cb.Frames[0]);
		}
	}

	[Test]
	public void ParseShortText()
	{
		var text = FillRandomASCII(125);
		using (var parser = new WebSocketClientParser())
		{
			var cb = new ParserCallback();
			SendFrameToParser(parser,
				new WebSocketFrame()
				{
					IsLastFragment = true,
					OpCode = MessageType.Text,
					Mask = new byte[] { 10, 20, 30, 40 },
					Payload = text,
				},
				cb.Callback);

            var expected = new WebSocketClientFrame(MessageType.Text, text);
			Assert.AreEqual(1, cb.Frames.Count);
			Assert.AreEqual(expected, cb.Frames[0]);
			Assert.AreEqual(Encoding.ASCII.GetString(text), cb.Frames[0].Text);
		}
	}

	[Test]
	public void ParseShortText126()
	{
		var text = FillRandomASCII(126);
		using (var parser = new WebSocketClientParser())
		{
			var cb = new ParserCallback();
			SendFrameToParser(parser,
				new WebSocketFrame()
				{
					IsLastFragment = true,
					OpCode = MessageType.Text,
					Mask = new byte[] { 10, 20, 30, 40 },
					Payload = text,
				},
				cb.Callback);

            var expected = new WebSocketClientFrame(MessageType.Text, text);
			Assert.AreEqual(1, cb.Frames.Count);
			Assert.AreEqual(expected, cb.Frames[0]);
		}
	}

	[Test]
	public void ParseShortText127()
	{
		var text = FillRandomASCII(127);
		using (var parser = new WebSocketClientParser())
		{
			var cb = new ParserCallback();
			SendFrameToParser(parser,
				new WebSocketFrame()
				{
					IsLastFragment = true,
					OpCode = MessageType.Text,
					Mask = new byte[] { 10, 20, 30, 40 },
					Payload = text,
				},
				cb.Callback);

            var expected = new WebSocketClientFrame(MessageType.Text, text);
			Assert.AreEqual(1, cb.Frames.Count);
			Assert.AreEqual(expected, cb.Frames[0]);
		}
	}

	[Test]
	public void ParseMediumText()
	{
		var text = FillRandomASCII(UInt16.MaxValue);
		using (var parser = new WebSocketClientParser())
		{
			var cb = new ParserCallback();
			SendFrameToParser(parser,
				new WebSocketFrame()
				{
					IsLastFragment = true,
					OpCode = MessageType.Text,
					Mask = new byte[] { 10, 20, 30, 40 },
					Payload = text,
				},
				cb.Callback);

            var expected = new WebSocketClientFrame(MessageType.Text, text);
			Assert.AreEqual(1, cb.Frames.Count);
			Assert.AreEqual(expected, cb.Frames[0]);
		}
	}

	[Test]
	public void ParseLongText()
	{
		var text = FillRandomASCII(UInt16.MaxValue + 1);
		using (var parser = new WebSocketClientParser())
		{
			var cb = new ParserCallback();
			SendFrameToParser(parser,
				new WebSocketFrame()
				{
					IsLastFragment = true,
					OpCode = MessageType.Text,
					Mask = new byte[] { 10, 20, 30, 40 },
					Payload = text,
				},
				cb.Callback);

            var expected = new WebSocketClientFrame(MessageType.Text, text);
			Assert.AreEqual(1, cb.Frames.Count);
			Assert.AreEqual(expected, cb.Frames[0]);
		}
	}

	[Test]
	public void ParseBytes()
	{
		var data = FillRandomBytes(100);
		using (var parser = new WebSocketClientParser())
		{
			var cb = new ParserCallback();
			SendFrameToParser(parser,
				new WebSocketFrame()
				{
					IsLastFragment = true,
					OpCode = MessageType.Binary,
					Mask = new byte[] { 10, 20, 30, 40 },
					Payload = data,
				},
				cb.Callback);

            var expected = new WebSocketClientFrame(MessageType.Binary, data);
			Assert.AreEqual(1, cb.Frames.Count);
			Assert.AreEqual(expected, cb.Frames[0]);
		}
	}

	[Test]
	public void ParseMultipleFrames()
	{

		var text = FillRandomASCII(150);
		var data = FillRandomBytes(100);
		using (var parser = new WebSocketClientParser())
		{
			var cb = new ParserCallback();

			// Dump the two frames into the same array, make sure that we
			// include the contents of one and then the other in the same feed
			// call.
			var buffer = new MemoryStream();
			new WebSocketFrame()
			{
				IsLastFragment = true,
				OpCode = MessageType.Text,
				Mask = new byte[] { 10, 20, 30, 40 },
				Payload = text,
			}.Write(buffer);
			new WebSocketFrame()
			{
				IsLastFragment = true,
				OpCode = MessageType.Binary,
				Mask = new byte[] { 10, 20, 30, 40 },
				Payload = data,
			}.Write(buffer);

			SendArrayToParser(parser, buffer.ToArray(), cb.Callback, 7);

            var expected1 = new WebSocketClientFrame(MessageType.Text, text);
            var expected2 = new WebSocketClientFrame(MessageType.Binary, data);
			Assert.AreEqual(2, cb.Frames.Count);
			Assert.AreEqual(expected1, cb.Frames[0]);
			Assert.AreEqual(expected2, cb.Frames[1]);
		}
	}

	[Test]
	public void ParseContinuation()
	{

		var text1 = FillRandomASCII(150);
		var text2 = FillRandomASCII(150);
		var text3 = FillRandomASCII(150);
		using (var parser = new WebSocketClientParser())
		{
			var cb = new ParserCallback();

			// Dump the two frames into the same array, make sure that we
			// include the contents of one and then the other in the same feed
			// call.
			var buffer = new MemoryStream();
			new WebSocketFrame()
			{
				IsLastFragment = false,
				OpCode = MessageType.Text,
				Mask = new byte[] { 10, 20, 30, 40 },
				Payload = text1,
			}.Write(buffer);
			new WebSocketFrame()
			{
				IsLastFragment = false,
				OpCode = MessageType.Continuation,
				Mask = new byte[] { 40, 30, 20, 10 },
				Payload = text2,
			}.Write(buffer);
			new WebSocketFrame()
			{
				IsLastFragment = true,
				OpCode = MessageType.Continuation,
				Mask = new byte[] { 30, 10, 40, 20 },
				Payload = text3,
			}.Write(buffer);

			SendArrayToParser(parser, buffer.ToArray(), cb.Callback, 7);

            var expected = new WebSocketClientFrame(MessageType.Text, text1.Concat(text2).Concat(text3).ToArray());
			Assert.AreEqual(1, cb.Frames.Count);
			Assert.AreEqual(expected, cb.Frames[0]);
		}
	}

	[Test]
	public void ParseContinuationWithControl()
	{

		var text1 = FillRandomASCII(150);
		var text2 = FillRandomASCII(150);
		var text3 = FillRandomASCII(150);
		var data1 = FillRandomBytes(50);
		var data2 = FillRandomBytes(50);
		using (var parser = new WebSocketClientParser())
		{
			var cb = new ParserCallback();

			// Dump the two frames into the same array, make sure that we
			// include the contents of one and then the other in the same feed
			// call.
			var buffer = new MemoryStream();
			new WebSocketFrame()
			{
				IsLastFragment = false,
				OpCode = MessageType.Text,
				Mask = new byte[] { 10, 20, 30, 40 },
				Payload = text1,
			}.Write(buffer);
			new WebSocketFrame()
			{
				IsLastFragment = true,
				OpCode = MessageType.Ping,
				Mask = new byte[] { 1, 2, 3, 4 },
				Payload = data1,
			}.Write(buffer);
			new WebSocketFrame()
			{
				IsLastFragment = false,
				OpCode = MessageType.Continuation,
				Mask = new byte[] { 40, 30, 20, 10 },
				Payload = text2,
			}.Write(buffer);
			new WebSocketFrame()
			{
				IsLastFragment = true,
				OpCode = MessageType.Ping,
				Mask = new byte[] { 4, 3, 2, 1 },
				Payload = data2,
			}.Write(buffer);
			new WebSocketFrame()
			{
				IsLastFragment = true,
				OpCode = MessageType.Continuation,
				Mask = new byte[] { 30, 10, 40, 20 },
				Payload = text3,
			}.Write(buffer);

			SendArrayToParser(parser, buffer.ToArray(), cb.Callback, 7);

            var expected1 = new WebSocketClientFrame(MessageType.Ping, data1);
            var expected2 = new WebSocketClientFrame(MessageType.Ping, data2);
            var expected3 = new WebSocketClientFrame(MessageType.Text, text1.Concat(text2).Concat(text3).ToArray());
			Assert.AreEqual(3, cb.Frames.Count);
			Assert.AreEqual(expected1, cb.Frames[0]);
			Assert.AreEqual(expected2, cb.Frames[1]);
			Assert.AreEqual(expected3, cb.Frames[2]);
		}
	}
}
