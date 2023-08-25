// -*- mode: csharp; fill-column: 100 -*-
using System.Text;

namespace brokerlib.tests;

internal struct FrameData
{
	public WebSocketFrame Frame { get; private set; }
	public byte[]? Data { get; private set; }

	public FrameData(WebSocketFrame frame, byte[]? data)
	{
		Frame = frame;
		Data = data;
	}
}

public class WebSocketClientTests : TestUtil
{
	private class ParserCallback
	{
		public List<WebSocketMessage> Frames = new List<WebSocketMessage>();

		public void Callback(WebSocketMessage frame)
		{
			Frames.Add(frame.ToOwned());
		}
	}

    private void SendFrameToParser(WebSocketClientParser parser, WebSocketFrame frame, byte[]? payload, Action<WebSocketMessage> callback, int chunkSize = -1)
	{
		SendArrayToParser(parser, frame.ToArray(payload), callback, chunkSize);
	}

    private void SendArrayToParser(WebSocketClientParser parser, byte[] buffer, Action<WebSocketMessage> callback, int chunkSize = -1)
	{
		var feedBuffer = parser.RentFeedBuffer();
		if (chunkSize <= 0 || chunkSize > feedBuffer.Count) chunkSize = feedBuffer.Count;

		foreach (var slice in ChunkBuffer(buffer, chunkSize))
		{
			slice.CopyTo(feedBuffer);
			parser.Feed(slice.Length, callback);
		}
	}

    private byte[] BuildFrames(params FrameData[] frames)
	{
		var totalSize = frames.Select(fd => fd.Frame.RequiredCapacity()).Sum();
		var buffer = new byte[totalSize];
		var bufferSpan = new Span<byte>(buffer);

		var offset = 0;
		foreach (var fd in frames)
		{
			var frameSize = fd.Frame.RequiredCapacity();
			var dataSpan = fd.Frame.WriteHeaderTo(bufferSpan.Slice(offset, frameSize));
			fd.Data.CopyTo(dataSpan);
			fd.Frame.MaskPayloadInBuffer(dataSpan);
			offset += frameSize;
		}

		return buffer;
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
					Payload = 0,
				},
				null,
				cb.Callback);

            var expected = new WebSocketMessage(MessageType.Text, new byte[0]);
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
					Payload = 0,
				},
				null,
				cb.Callback);

            var expected = new WebSocketMessage(MessageType.Text, new byte[0]);
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
					Payload = 0,
				},
				null,
				cb.Callback);

            var expected = new WebSocketMessage(MessageType.Binary, new byte[0]);
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
					Payload = 0,
				},
				null,
				cb.Callback);

            var expected = new WebSocketMessage(MessageType.Close, new byte[0]);
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
					Payload = 0,
				},
				null,
				cb.Callback);

            var expected = new WebSocketMessage(MessageType.Ping, new byte[0]);
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
					Payload = 0,
				},
				null,
				cb.Callback);

            var expected = new WebSocketMessage(MessageType.Pong, new byte[0]);
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
					Payload = text.Length,
				},
				text,
				cb.Callback);

            var expected = new WebSocketMessage(MessageType.Text, text);
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
					Payload = text.Length,
				},
				text,
				cb.Callback);

            var expected = new WebSocketMessage(MessageType.Text, text);
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
					Payload = text.Length,
				},
				text,
				cb.Callback);

            var expected = new WebSocketMessage(MessageType.Text, text);
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
					Payload = text.Length,
				},
				text,
				cb.Callback);

            var expected = new WebSocketMessage(MessageType.Text, text);
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
					Payload = text.Length,
				},
				text,
				cb.Callback);

            var expected = new WebSocketMessage(MessageType.Text, text);
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
					Payload = data.Length,
				},
				data,
				cb.Callback);

            var expected = new WebSocketMessage(MessageType.Binary, data);
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

			var frame1 = new WebSocketFrame()
			{
				IsLastFragment = true,
				OpCode = MessageType.Text,
				Mask = new byte[] { 10, 20, 30, 40 },
				Payload = text.Length,
			};
			var frame2 = new WebSocketFrame()
			{
				IsLastFragment = true,
				OpCode = MessageType.Binary,
				Mask = new byte[] { 10, 20, 30, 40 },
				Payload = data.Length,
			};

            var buffer = BuildFrames(new FrameData(frame1, text), new FrameData(frame2, data));
			SendArrayToParser(parser, buffer, cb.Callback, 7);

            var expected1 = new WebSocketMessage(MessageType.Text, text);
            var expected2 = new WebSocketMessage(MessageType.Binary, data);
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

			var frame1 = new WebSocketFrame()
			{
				IsLastFragment = false,
				OpCode = MessageType.Text,
				Mask = new byte[] { 10, 20, 30, 40 },
				Payload = text1.Length,
			};
			var frame2 = new WebSocketFrame()
			{
				IsLastFragment = false,
				OpCode = MessageType.Continuation,
				Mask = new byte[] { 40, 30, 20, 10 },
				Payload = text2.Length,
			};
			var frame3 = new WebSocketFrame()
			{
				IsLastFragment = true,
				OpCode = MessageType.Continuation,
				Mask = new byte[] { 30, 10, 40, 20 },
				Payload = text3.Length,
			};
            var buffer = BuildFrames(new FrameData(frame1, text1),
									 new FrameData(frame2, text2),
									 new FrameData(frame3, text3));

			SendArrayToParser(parser, buffer, cb.Callback, 7);

            var expected = new WebSocketMessage(MessageType.Text, text1.Concat(text2).Concat(text3).ToArray());
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
			var frame1 = new WebSocketFrame()
			{
				IsLastFragment = false,
				OpCode = MessageType.Text,
				Mask = new byte[] { 10, 20, 30, 40 },
				Payload = text1.Length,
			};
			var frame2 = new WebSocketFrame()
			{
				IsLastFragment = true,
				OpCode = MessageType.Ping,
				Mask = new byte[] { 1, 2, 3, 4 },
				Payload = data1.Length,
			};
			var frame3 = new WebSocketFrame()
			{
				IsLastFragment = false,
				OpCode = MessageType.Continuation,
				Mask = new byte[] { 40, 30, 20, 10 },
				Payload = text2.Length,
			};
			var frame4 = new WebSocketFrame()
			{
				IsLastFragment = true,
				OpCode = MessageType.Ping,
				Mask = new byte[] { 4, 3, 2, 1 },
				Payload = data2.Length,
			};
			var frame5 = new WebSocketFrame()
			{
				IsLastFragment = true,
				OpCode = MessageType.Continuation,
				Mask = new byte[] { 30, 10, 40, 20 },
				Payload = text3.Length,
			};

            var buffer = BuildFrames(new FrameData(frame1, text1),
									 new FrameData(frame2, data1),
									 new FrameData(frame3, text2),
									 new FrameData(frame4, data2),
									 new FrameData(frame5, text3));
			SendArrayToParser(parser, buffer.ToArray(), cb.Callback, 7);

            var expected1 = new WebSocketMessage(MessageType.Ping, data1);
            var expected2 = new WebSocketMessage(MessageType.Ping, data2);
            var expected3 = new WebSocketMessage(MessageType.Text, text1.Concat(text2).Concat(text3).ToArray());
			Assert.AreEqual(3, cb.Frames.Count);
			Assert.AreEqual(expected1, cb.Frames[0]);
			Assert.AreEqual(expected2, cb.Frames[1]);
			Assert.AreEqual(expected3, cb.Frames[2]);
		}
	}
}
