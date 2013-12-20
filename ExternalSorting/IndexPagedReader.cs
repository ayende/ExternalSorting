using System;
using System.IO;
using System.Text;

namespace ExternalSorting
{
	public class IndexPagedReader
	{
		public const int PageSize = 4096;
		private readonly Stream _index;
		private readonly Encoding _encoding;
		private long _page;

		private IndexEntry _entry = new IndexEntry();
		private char[] _charBuf = new char[0];
		private readonly byte[] _buffer = new byte[PageSize];

		public IndexPagedReader(Stream index, Encoding encoding)
		{
			_index = index;
			_encoding = encoding;
			NumberOfPages = _index.Length / PageSize;
		}

		public long NumberOfPages { get; private set; }

		public long Page
		{
			get { return _page; }
			set
			{
				_index.Seek(value * PageSize, SeekOrigin.Begin);
				_page = value;
			}
		}

		public IndexEntry Read()
		{
			// at end of page
			if (_index.Position >= (_page + 1) * PageSize)
				return null;

			Read(2);// read len
			var len = BitConverter.ToInt16(_buffer, 0);
			Read(len);
			if (len == 0)
				return null; // nothing else in this page
			var maxCharCount = _encoding.GetMaxCharCount(len);
			if(maxCharCount > _charBuf.Length)
				_charBuf = new char[Utils.NearestPowerOfTwo(maxCharCount)];
			var read = _encoding.GetChars(_buffer, 0, len, _charBuf, 0);
			_entry.Value = new ArraySegment<char>(_charBuf, 0, read);
			Read(8);
			_entry.Position = BitConverter.ToInt64(_buffer, 0);

			return _entry;
		}

		private void Read(int size)
		{
			int pos = 0;
			while (size > 0)
			{
				var read = _index.Read(_buffer, pos, size);
				if (read == 0)
				{
					throw new EndOfStreamException();
				}
				pos += read;
				size -= read;
			}
		}
	}
}