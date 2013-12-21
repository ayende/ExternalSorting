using System;

namespace ExternalSorting
{
	public class IndexEntry
	{
		public ArraySegment<char> Value;
		public long Position;

		public override string ToString()
		{
			return new string(Value.Array, Value.Offset, Value.Count);
		}
	}
}