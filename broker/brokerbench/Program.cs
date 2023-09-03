// -*- mode: csharp; fill-column: 100 -*-
using brokerlib;
using BenchmarkDotNet.Running;
using BenchmarkDotNet.Attributes;

namespace brokerlib.benchmark;

public class ClientFrameParser
{
    [Params(1)]
    public int Frames { get; set; }

    [Params(4096)]
    public int ChunkSize { get; set; }

    [Params(0, 100, 1000)]
    public int DataSize { get; set; }

    private byte[] FrameData;

    [GlobalSetup]
    public void BuildMessageBuffer()
    {
        var data = new byte[DataSize];
        Random.Shared.NextBytes(data);

        var frame = new WebSocketFrame()
        {
            IsLastFragment = true,
            OpCode = MessageType.Binary,
            Mask = new byte[] { 1, 2, 3, 4 },
            Payload = data.Length,
        };
        FrameData = frame.ToArray(data);
    }

	/// Chunks a buffer into pieces with the given maximum size.
	private IEnumerable<Memory<byte>> ChunkBuffer(byte[] buffer, int chunkSize)
	{
		var offset = 0;
		while (offset < buffer.Length)
		{
			var thisChunk = Math.Min(buffer.Length - offset, chunkSize);
			yield return new Memory<byte>(buffer, offset, thisChunk);
			offset += thisChunk;
		}
	}

    private void ParserCallback(WebSocketMessage message)
    {
    }

    [Benchmark]
    public void ParseFrames()
    {
        for (var i = 0; i < Frames; i++)
        {
            using (var parser = new WebSocketClientParser())
            {
                var buffer = parser.RentFeedBuffer();
                foreach (var chunk in ChunkBuffer(FrameData, ChunkSize))
                {
                    chunk.CopyTo(buffer);
                    parser.Feed(chunk.Length, ParserCallback);
                }
            }
        }
    }
}

public class FrameBuilding
{
    [Params(1)]
    public int Frames { get; set; }

    [Params(0, 100, 1000)]
    public int DataSize { get; set; }

    private WebSocketFrame Frame;
    private byte[] Buffer;

    [GlobalSetup]
    public void BuildMessageBuffer()
    {
        var data = new byte[DataSize];
        Random.Shared.NextBytes(data);

        Frame = new WebSocketFrame()
        {
            IsLastFragment = true,
            OpCode = MessageType.Binary,
            Mask = new byte[] { 1, 2, 3, 4 },
            Payload = data.Length,
        };
        Buffer = new byte[Frame.RequiredCapacity()];
    }

    [Benchmark(Baseline=true)]
    public void FillFrameData()
    {
        var dataSpan = Frame.WriteHeaderTo(new Span<byte>(Buffer));
        for (var i = 0; i < Frames; i++)
        {
            dataSpan.Fill(42);
        }
    }

    [Benchmark]
    public void BuildFrames()
    {
        for (var i = 0; i < Frames; i++)
        {
            var dataSpan = Frame.WriteHeaderTo(new Span<byte>(Buffer));
            dataSpan.Fill(42);
            Frame.MaskPayloadInBuffer(dataSpan);
        }
    }
}

public class CommandBuilding
{
    [Params(1)]
    public int Commands { get; set; }

    [Params(0, 100, 1000)]
    public int DataSize { get; set; }

    private SendBrokerCommand Command;
    private byte[] Buffer;
    private byte[] Data;

    [GlobalSetup]
    public void BuildMessageBuffer()
    {
        var data = new byte[DataSize];
        Random.Shared.NextBytes(data);

        Command = new SendBrokerCommand()
        {
            Id = 1,
            Command = new ArraySegment<byte>(data),
        };
        Buffer = new byte[Command.EncodedSize()];
    }

    [Benchmark]
    public void BuildCommands()
    {
        for (var i = 0; i < Commands; i++)
        {
            Command.EncodeTo(new ArraySegment<byte>(Buffer));
        }
    }
}

public class CommandParsing
{
    [Params(1)]
    public int Commands { get; set; }

    [Params(0, 100, 1000)]
    public int DataSize { get; set; }

    private byte[] Buffer;

    [GlobalSetup]
    public void BuildCommandBuffer()
    {
        var data = new byte[DataSize];
        Random.Shared.NextBytes(data);

        var command = new SendBrokerCommand()
        {
            Id = 1,
            Command = data,
        };

        Buffer = new byte[command.EncodedSize()];
        command.EncodeTo(Buffer);
    }

    [Benchmark]
    public void ParseFrames()
    {
        for (var i = 0; i < Commands; i++)
        {
            var command = BrokerCommand.Decode(new Span<byte>(Buffer));
        }
    }
}

public class Program
{
    public static void Main(string[] args)
    {
        BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
    }
}
