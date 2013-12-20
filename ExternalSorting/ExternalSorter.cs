using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace ExternalSorting
{
	public class ExternalSorter
	{
		private readonly Stream _csv;
		private readonly ExternalStorageOptions _options;
		private readonly int[] _columns;
		private readonly CharPool _pool = new CharPool();
		private readonly List<IndexState> _indexes = new List<IndexState>();

		private class IndexState
		{
			public readonly List<IndexEntry> Values = new List<IndexEntry>();
			public long Counter;
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
			long lastFlushPosition = 0;
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
					Buffer.BlockCopy(value.Array, value.Offset * sizeof(char), checkout, 0, value.Count * sizeof(char));
					var arraySegment = new ArraySegment<char>(checkout, 0, value.Count);

					indexState.Values.Add(new IndexEntry
					{
						Position = result.Position,
						Value = arraySegment
					});
				}

				if ((_csv.Position - lastFlushPosition) > _options.FlushIndexesInternval)
				{
					lastFlushPosition = _csv.Position;
					FlushIntermediateIndexes();
					Console.Write("\r{0:#,#;;0} kb out of {1:#,#;;0} kb = {2:P2}", result.Position / 1024, _csv.Length / 1024, result.Position / (decimal)_csv.Length);
				}
			}
			FlushIntermediateIndexes();
			Console.WriteLine();

			MergePartialIndexes();
		}

		private void MergePartialIndexes()
		{
			for (int index = 0; index < _indexes.Count; index++)
			{
				var partialReaders = _options.GetAllPartialsFor(index)
					.Select(x => new IndexPagedReader(x, _options.Encoding))
					.ToList();

				var heap = new Heap<HeapEntry>(partialReaders.Count, (x, y) => CompareIndexEntries(x.IndexEntry, y.IndexEntry));
				foreach (var reader in partialReaders)
				{
					var entry = reader.Read();
					if (entry == null) // empty reader?
						continue;
					heap.Enqueue(new HeapEntry
					{
						Reader = reader,
						IndexEntry = entry
					});
				}
				using (var stream = _options.Create(index))
				using (var builder = new IndexBuilder(stream, _options.Encoding))
				{
					while (heap.Count > 0)
					{
						var heapEntry = heap.Dequeue();
						builder.Add(heapEntry.IndexEntry);
						heapEntry.IndexEntry = heapEntry.Reader.Read();
						if (heapEntry.IndexEntry == null)
						{
							if (heapEntry.Reader.Page +1 >= heapEntry.Reader.NumberOfPages)
								continue;
							heapEntry.Reader.Page++;
							heapEntry.IndexEntry = heapEntry.Reader.Read();
						}
						heap.Enqueue(heapEntry);
					}
				}
			}
		}

		private class HeapEntry
		{
			public IndexPagedReader Reader;
			public IndexEntry IndexEntry;
		}

		private void FlushIntermediateIndexes()
		{
			for (int index = 0; index < _indexes.Count; index++)
			{
				var indexState = _indexes[index];
				indexState.Values.Sort(CompareIndexEntries);
				indexState.Counter++;
				using (var stream = _options.CreatePartial(index, indexState.Counter))
				using (var indexBuilder = new IndexBuilder(stream, _options.Encoding))
				{
					foreach (var indexEntry in indexState.Values)
					{
						indexBuilder.Add(indexEntry);
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