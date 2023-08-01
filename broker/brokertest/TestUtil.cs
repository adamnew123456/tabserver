public abstract class TestUtil
{
	protected byte[] FillRandomBytes(int size)
	{
		var body = new byte[size];
		Random.Shared.NextBytes(body);
		return body;
	}

	protected byte[] FillRandomASCII(int size)
	{
		var body = new byte[size];
		Random.Shared.NextBytes(body);

		var range = '~' - ' ';
		for (var i = 0; i < body.Length; i++)
		{
			var offset = body[i] % range;
			body[i] = (byte)(' ' + offset);
		}
		return body;
	}
}
