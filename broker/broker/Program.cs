using System.Net;
using System.Threading;

using brokerlib;
namespace broker;

public class Program
{
    public static void Main(string[] args)
    {
        if (args.Length != 2)
        {
            Console.WriteLine("Usage: broker TABSERVER-PORT UPSTREAM-PORT");
            Environment.Exit(1);
        }

        var tabserverPort = int.Parse(args[0]);
        var upstreamPort = int.Parse(args[1]);

        if (tabserverPort == upstreamPort)
        {
            Console.WriteLine("TABSERVER-PORT and UPSTREAM-PORT must be different");
            Environment.Exit(1);
        }

        var tabserverEndpoint = new IPEndPoint(IPAddress.Any, tabserverPort);
        var upstreamEndpoint = new IPEndPoint(IPAddress.Any, upstreamPort);

        var eventDispatcher = new BrokerEventDispatcher();
        var dispatchThread = eventDispatcher.StartThread(new AsyncSocketManager(), upstreamEndpoint, tabserverEndpoint);

        Console.CancelKeyPress += (object? sender, ConsoleCancelEventArgs evt) =>
        {
            Console.WriteLine("Telling broker to stop");
            eventDispatcher.PostEvent(StopBroker.Instance);
            Console.WriteLine("Waiting for broker to stop");
            dispatchThread.Join();
        };

        Console.WriteLine($"Tabserver endpoint: {tabserverEndpoint}");
        Console.WriteLine($"Upstream endpoint: {upstreamEndpoint}");
        Console.WriteLine("Running broker");
        dispatchThread.Join();
    }
}
