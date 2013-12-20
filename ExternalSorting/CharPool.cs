using System.Collections.Generic;

namespace ExternalSorting
{
	public class CharPool
	{
		private readonly Dictionary<int, LinkedList<char[]>> _pool = new Dictionary<int, LinkedList<char[]>>();

		public char[] Checkout(int size)
		{
			size = (int)Utils.NearestPowerOfTwo(size);

			LinkedList<char[]> list;
			if (_pool.TryGetValue(size, out list) == false || list.Count == 0)
				return new char[size];

			var value = list.First.Value;
			list.RemoveFirst();
			return value;
		}

		public void Return(char[] buf)
		{
			LinkedList<char[]> list;
			if (_pool.TryGetValue(buf.Length, out list) == false)
			{
				_pool[buf.Length] = list = new LinkedList<char[]>();
			}
			list.AddFirst(buf);
		}
	}
}