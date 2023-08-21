// -*- mode: csharp; fill-column: 100 -*-
using System.Text.Json.Serialization;

namespace brokerlib;

public class EncodedCommand : IEquatable<EncodedCommand>
{
    [JsonPropertyName("id")]
    public string? Id { get; set; }

    [JsonPropertyName("line")]
    public string? Command { get; set; }

    public bool Equals(EncodedCommand? other)
    {
        if (other == null) return false;
        return this.Id == other.Id && other.Command == this.Command;
    }

    public override string ToString()
    {
        return $"EncodedCommand<from={Id}, message={Command}>";
    }
}
