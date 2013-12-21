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
					Array.Copy(value.Array, value.Offset, checkout, 0, value.Count);
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
					.Select(x => new SourceReader(x, _options.Encoding, new[] { 0, 1 }).ReadFromStream().GetEnumerator())
					.ToList();

				var heap = new Heap<HeapEntry>(partialReaders.Count, (x, y) => Utils.CompareIndexEntries(x.IndexEntry, y.IndexEntry));
				for (int i = 0; i < partialReaders.Count; i++)
				{
					var reader = partialReaders[i];
					if (reader.MoveNext() == false) // empty reader?
						continue;
					var heapEntry = new HeapEntry
					{
						Index = index,
						Reader = reader,
						IndexEntry = ReadIndexEntry(reader)
					};
					heap.Enqueue(heapEntry);
				}

				using (var stream = _options.Create(index))
				using (var builder = new IndexBuilder(stream, _options.Encoding))
				{
					while (heap.Count > 0)
					{
						var heapEntry = heap.Dequeue();
						builder.Add(heapEntry.IndexEntry);
						if (heapEntry.Reader.MoveNext() == false)
							continue;
						heapEntry.IndexEntry = ReadIndexEntry(heapEntry.Reader);
						heap.Enqueue(heapEntry);
					}
				}

				_options.DeleteAllPartialsFor(index);
			}
		}

		private IndexEntry ReadIndexEntry(IEnumerator<SourceReader.Result> reader)
		{
			var p = Utils.ToInt64(reader.Current.Values[1]);

			return new IndexEntry
			{
				Value = reader.Current.Values[0],
				Position = p
			};
		}

		

		public class HeapEntry
		{
			public IEnumerator<SourceReader.Result> Reader;
			public IndexEntry IndexEntry;
			public int Index;

			public override string ToString()
			{
				return IndexEntry.ToString();
			}
		}

		private void FlushIntermediateIndexes()
		{
			for (int index = 0; index < _indexes.Count; index++)
			{
				var indexState = _indexes[index];
				indexState.Values.Sort(Utils.CompareIndexEntries);
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
	}
}