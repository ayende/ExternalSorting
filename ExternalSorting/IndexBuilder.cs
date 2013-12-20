﻿using System;
using System.IO;
using System.Text;

namespace ExternalSorting
{
	/// <summary>
	/// This class writes to the output stream the values.
	/// It is assumed that the values are sorted (although that isn't actually required for what we are doing
	/// No single Value is greater than 4086 bytes
	/// The values are written in pages, so that each page may contain multiple values, but no Value will ever
	/// cross a page boundary.
	/// The format is:
	/// short - len
	/// byte[len] - data
	/// long - line position
	/// end of page is when the page size is over or we encounter a len of 0
	/// </summary>
	public class IndexBuilder : IDisposable
	{
		private readonly Stream _output;
		private readonly Encoding _encoding;
		private readonly byte[] _buffer = new byte[4096];
		private int _bufferPos;

		public IndexBuilder(Stream output, Encoding encoding)
		{
			_output = output;
			_encoding = encoding;
		}

		public unsafe void Add(ArraySegment<char> value, long pos)
		{
			Console.WriteLine(new string(value.Array, value.Offset, value.Count));
			var byteCount = _encoding.GetByteCount(value.Array, value.Offset, value.Count);

			var requiredSize = (byteCount+sizeof(long) + sizeof(short));
			if (requiredSize > 4094)
				throw new InvalidOperationException("Value too large");

			if (requiredSize > _buffer.Length)
			{
				Flush();
			}

			fixed (byte* bp = _buffer)
			{
				*((short*) (bp+_bufferPos)) = (short) byteCount;
				_bufferPos += sizeof (short);
				_encoding.GetBytes(value.Array, value.Offset, value.Count, _buffer, _bufferPos);
				_bufferPos += byteCount;
				*((long*)(bp + _bufferPos)) = pos;
				_bufferPos += sizeof (long);
			}
		}

		private void Flush()
		{
			_output.Write(_buffer, 0, _buffer.Length);
			Array.Clear(_buffer, 0, _buffer.Length);
			_bufferPos = 0;
		}

		public void Dispose()
		{
			Flush();
		}
	}
}