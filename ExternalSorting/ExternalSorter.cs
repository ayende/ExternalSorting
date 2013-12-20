using System;
using System.Collections.Generic;
using System.IO;

namespace ExternalSorting
{
	public class ExternalSorter
	{
		private readonly Stream _csv;
		private readonly ExternalStorageOptions _options;
		private readonly int[] _columns;
		private readonly CharPool _pool = new CharPool();
		private long _heldMemory;
		private readonly List<IndexState> _indexes = new List<IndexState>();

		private class IndexState
		{
			public readonly List<IndexEntry> Values = new List<IndexEntry>();
			public long Counter;
		}

		public class IndexEntry
		{
			public ArraySegment<char> Value;
			public long Position;

		}

		public ExternalSorter(Stream csv, ExternalStorageOptions options, params int[] columns)
		{
			_csv = csv;
			_options = options;
			_columns = columns;
			for (int index = 0; index < columns.Length; index++)
			{
				_indexes.Add(new IndexState());
			}
		}

		public void Sort()
		{
			var reader = new SourceReader(_csv, _options.Encoding, _columns);
			var readHeader = false;
			foreach (var result in reader.ReadFromStream())
			{
				if (readHeader == false)
				{
					readHeader = true;
					continue;
				}

				for (int index = 0; index < result.Values.Count; index++)
				{
					var value = result.Values[index];
					var indexState = _indexes[index];
					var checkout = _pool.Checkout(value.Count);
					Buffer.BlockCopy(value.Array, value.Offset, checkout, 0, value.Count);
					indexState.Values.Add(new IndexEntry
					{
						Position = result.Position,
						Value = new ArraySegment<char>(checkout, 0, value.Count)
					});
					_heldMemory += value.Count;
				}

				if (_heldMemory >= _options.MaxHeldMemory)
				{
					FlushIntermediateIndexes();
				}
			}
			FlushIntermediateIndexes();
		}

		private void FlushIntermediateIndexes()
		{
			for (int index = 0; index < _indexes.Count; index++)
			{
				var indexState = _indexes[index];
				indexState.Values.Sort(CompareIndexEntries);
				indexState.Counter++;
				using (var stream = _options.Create(index, indexState.Counter))
				using (var indexBuilder = new IndexBuilder(stream, _options.Encoding))
				{
					foreach (var indexEntry in indexState.Values)
					{
						indexBuilder.Add(indexEntry.Value, indexEntry.Position);
						_pool.Return(indexEntry.Value.Array);
					}
					indexState.Values.Clear();
				}
			}
		}

		private int CompareIndexEntries(IndexEntry x, IndexEntry y)
		{
			var minSize = Math.Min(x.Value.Count, y.Value.Count);

			for (int i = 0; i < minSize; i++)
			{
				var diff =
					(
						x.Value.Array[i + x.Value.Offset] -
						y.Value.Array[i + y.Value.Offset]
					);
				if (diff != 0)
					return diff;
			}

			return x.Value.Count - y.Value.Count;
		}
	}
}