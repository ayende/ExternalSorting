using System;

namespace ExternalSorting
{
	public class Utils
	{
		public static long NearestPowerOfTwo(long v)
		{
			v--;
			v |= v >> 1;
			v |= v >> 2;
			v |= v >> 4;
			v |= v >> 8;
			v |= v >> 16;
			v++;
			return v;
		}

		public static long ToInt64(ArraySegment<char> pos)
		{
			long p = 0;
			for (int i = 0; i < pos.Count; i++)
			{
				p = p * 10 + (pos.Array[i + pos.Offset] - '0');
			}
			return p;
		}

		public static int CompareIndexEntries(IndexEntry x, IndexEntry y)
		{
			var xSeg = x.Value;
			var ySeg = y.Value;
			return CompareArraySegments(xSeg, ySeg);
		}

		public static int CompareArraySegments(ArraySegment<char> xSeg, ArraySegment<char> ySeg)
		{
			var minSize = Math.Min(xSeg.Count, ySeg.Count);

			for (int i = 0; i < minSize; i++)
			{
				var diff =
					(
						xSeg.Array[i + xSeg.Offset] -
						ySeg.Array[i + ySeg.Offset]
						);
				if (diff != 0)
					return diff;
			}

			return xSeg.Count - ySeg.Count;
		}
	}
}