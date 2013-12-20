using System;
using System.IO;

namespace ExternalSorting
{
	/// <summary>
	/// This class writes to the output stream the values.
	/// It is assumed that the values are sorted (although that isn't actually required for what we are doing
	/// No single value is greater than 4094 bytes
	/// The values are written in pages, so that each page may contain multiple values, but no value will ever
	/// cross a page boundary.
	/// The format is:
	/// short - len
	/// byte[] - size of len
	/// end of page is when the page size is over or we encounter a len of 0
	/// </summary>
	public class PagedSortedOutput : IDisposable
	{
		private readonly Stream _output;
		private readonly byte[] _buffer = new byte[4096];
		private int _bufferPos;

		public PagedSortedOutput(Stream output)
		{
			_output = output;
		}

		public void Add(byte[] value)
		{
			if (value.Length > 4094)
				throw new InvalidOperationException("Value too large");

			if (_bufferPos + value.Length + 2 > _buffer.Length)
			{
				Flush();
			}

			var len = (short)value.Length;
			_buffer[_bufferPos++] = (byte)(len >> 8);
			_buffer[_bufferPos++] = (byte)(len);
			Buffer.BlockCopy(value, 0, _buffer, _bufferPos, value.Length);
			_bufferPos += value.Length;
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