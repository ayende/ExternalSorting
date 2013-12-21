using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace ExternalSorting
{
	public class SourceReader
	{
		private readonly Stream _input;
		private readonly Encoding _encoding;
		private readonly int[] _columns;
		private readonly Result _result;
		private char[] _charBuf;
		private readonly MemoryStream _buffer;

		/// <summary>
		/// Required: columns not empty and is in ascending order
		/// </summary>
		public SourceReader(Stream input, Encoding encoding, params int[] columns)
		{
			_input = input;
			_encoding = encoding;
			_columns = columns;
			_result = new Result
			{
				Values = new List<ArraySegment<char>>(columns.Length)
			};

			_charBuf = new char[0];
			_buffer = new MemoryStream();
		}

		public class Result
		{
			public List<ArraySegment<char>> Values;
			public long Position;
		}

		public Result Current { get { return _result; } }

		public IEnumerable<Result> ReadFromStream()
		{
			while (true)
			{
				switch (ReadOneLine())
				{
					case null:
						break;
					case false:
						yield break;
						break;
					case true:
						yield return _result;
						break;
				}
			}
		}

		public bool? ReadOneLine()
		{
			_buffer.SetLength(0);
			_result.Position = _input.Position;
			while (true)
			{
				var b = _input.ReadByte();
				if (b == -1)
					return false;

				if (b != '\n')
				{
					_buffer.WriteByte((byte)b);
					continue;
				}


				_result.Values.Clear();

				// empty line
				if (_buffer.Length == 1)
				{
					return null;
				}

				ParseLine();
				return true;
			}
		}

		public void SetPositionToLineAt(long position)
		{
			// now we need to go back until we either get to the start of the file
			// or find a \n character
			const int bufferSize = 128;
			_buffer.Capacity = Math.Max(bufferSize, _buffer.Capacity);

			var charCount = _encoding.GetMaxCharCount(bufferSize);
			if (charCount > _charBuf.Length)
				_charBuf = new char[Utils.NearestPowerOfTwo(charCount)];

			while (true)
			{
				_input.Position = position - (position < bufferSize ? 0 : bufferSize);
				var read = ReadToBuffer(bufferSize);
				var buffer = _buffer.GetBuffer();
				var chars = _encoding.GetChars(buffer, 0, read, _charBuf, 0);
				for (int i = chars - 1; i >= 0; i--)
				{
					if (_charBuf[i] == '\n')
					{
						_input.Position = position - (bufferSize - i) + 1;
						return;
					}
				}
				position -= bufferSize;
				if (position < 0)
				{
					_input.Position = 0;
					return;
				}
			}
		}

		private int ReadToBuffer(int bufferSize)
		{
			int pos = 0;
			while (pos < bufferSize)
			{
				var read = _input.Read(_buffer.GetBuffer(), pos, bufferSize - pos);
				if (read == 0)
					return pos;
				pos += bufferSize;
			}
			return bufferSize;
		}


		private void ParseLine()
		{
			EnsureCharBuffer();

			var read = _encoding.GetChars(_buffer.GetBuffer(), 0, (int)_buffer.Length, _charBuf, 0);

			if (_charBuf[0] == 65279)
			{

			}
			int columnPos = 0;
			int currentColumn = 0;
			var interestingColumnPos = 0;
			for (int i = 0; i < read && interestingColumnPos < _columns.Length; i++)
			{
				// search for comma, end of a column, or at end
				if (_charBuf[i] != ',' && _charBuf[i] != '\r')
					continue;

				// we care for this column
				if (_columns[interestingColumnPos] == currentColumn)
				{
					interestingColumnPos++;
					var value = _charBuf[columnPos] != '"'
						// not quoted
						? new ArraySegment<char>(_charBuf, columnPos, (i - columnPos))
						// quoted
						: new ArraySegment<char>(_charBuf, columnPos + 1, (i - columnPos) - 2);

					_result.Values.Add(value);
				}
				currentColumn++;
				columnPos = i + 1;
			}
		}

		private void EnsureCharBuffer()
		{
			var maxCharCount = _encoding.GetMaxCharCount((int)_buffer.Length);
			if (maxCharCount > _charBuf.Length)
				_charBuf = new char[Utils.NearestPowerOfTwo(maxCharCount)];
		}
	}
}