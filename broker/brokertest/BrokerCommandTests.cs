// -*- mode: csharp; fill-column: 100 -*-
using System.Text;

namespace brokerlib.tests;

public class BrokerCommandTests : TestUtil
{
    [Test]
    public void ParseHello()
    {
        var command = new HelloBrokerCommand()
        {
            Id = 1,
            Name = ArrayOfString("foo"),
        };
        var encoded = EncodeBrokerCommand(command);
        var decoded = BrokerCommand.Decode(encoded);
        Assert.AreEqual(command, decoded);
    }

    [Test]
    public void ParseGoodbye()
    {
        var command = new GoodbyeBrokerCommand()
        {
            Id = 1,
        };
        var encoded = EncodeBrokerCommand(command);
        var decoded = BrokerCommand.Decode(encoded);
        Assert.AreEqual(command, decoded);
    }

    [Test]
    public void ParseSend()
    {
        var command = new SendBrokerCommand()
        {
            Id = 1,
            Command = ArrayOfString("foo"),
        };
        var encoded = EncodeBrokerCommand(command);
        var decoded = BrokerCommand.Decode(encoded);
        Assert.AreEqual(command, decoded);
    }
}
