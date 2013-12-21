using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Eventing.Reader;
using System.IO;
using System.Linq;
using System.Text;

namespace ExternalSorting
{
	/// <summary>
	/// We expect a CSV file with two columns (no headers)
	/// First column is the actual value
	/// Second column is the position in the original file of the start of the line.
	/// </summary>
	public class IndexSearcher
	{
		private readonly Stream _data;
		private readonly Stream _index;
		private readonly Encoding _encoding;
		private readonly SourceReader _reader;
		private readonly MemoryStream _buffer = new MemoryStream();

		public IndexSearcher(Stream data, Stream index, Encoding encoding)
		{
			_data = data;
			_index = index;
			_encoding = encoding;

			_reader = new SourceReader(index, encoding, new []{0,1});
		}

		public IEnumerable<string> Search(string value)
		{
			var expectedIndexEntry = new ArraySegment<char>(value.ToCharArray());
			// binary search through the file
			var hi = _index.Length;
			long lo = 0;
			int match = -1;
			long position = -1;
			while (lo <= hi)
			{
				position = (lo + hi) / 2;
				_reader.SetPositionToLineAt(position);

				bool? result;
				do
				{
					result = _reader.ReadOneLine();
				} while (result == null); // skip empty lines

				if (result == false)
					yield break; // couldn't find anything

				var entry = _reader.Current.Values[0];
				match = Utils.CompareArraySegments(expectedIndexEntry, entry);

				if (match == 0)
				{
					break;
				}
				if (match > 0)
					lo = position + _reader.Current.Values.Sum(x => x.Count) + 1;
				else
					hi = position - 1;
			}

			if (match != 0)
			{
				// no match
				yield break;
			}

			// we have a match, now we need to return all the matches
			_reader.SetPositionToLineAt(position);

			while(true)
			{
				bool? result;
				do
				{
					result = _reader.ReadOneLine();
				} while (result == null); // skip empty lines

				if(result == false)
					yield break; // end of file

				var entry = _reader.Current.Values[0];
				match = Utils.CompareArraySegments(expectedIndexEntry, entry);
				if (match != 0)
					yield break; // out of the valid range we need

				_buffer.SetLength(0);
				_data.Position = Utils.ToInt64(_reader.Current.Values[1]);

				while (true)
				{
					var b = _data.ReadByte();
					if (b == -1)
						break;
					if (b == '\n')
					{
						break;
					}
					_buffer.WriteByte((byte)b);
				}

				yield return _encoding.GetString(_buffer.GetBuffer(), 0, (int)_buffer.Length);
			} 
		}
	}
}