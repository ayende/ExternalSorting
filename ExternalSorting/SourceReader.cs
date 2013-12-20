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
		private bool _lastCharWasLF;


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
			_lastCharWasLF = false;
		}

		public class Result
		{
			public List<ArraySegment<char>> Values;
			public long Position;
		}

		public IEnumerable<Result> ReadFromStream()
		{
			while (true)
			{
				var b = _input.ReadByte();
				if (b == -1)
					yield break; // we ignore the last line if it doesn't end with \r\n

				if (!_lastCharWasLF || b != '\n')
				{
					_lastCharWasLF = b == '\r';
					_buffer.WriteByte((byte) b);
					continue;
				}

				_lastCharWasLF = false;

				_result.Position = _input.Position;
				_result.Values.Clear();

				// empty line
				if (_buffer.Length == 1)
				{
					_buffer.SetLength(0);
					_lastCharWasLF = false;
					continue;
				}

				ParseLine();

				yield return _result;
			}
		}

		private void ParseLine()
		{
			EnsureCharBuffer();

			var read = _encoding.GetChars(_buffer.GetBuffer(), 0, (int) _buffer.Length, _charBuf, 0);
			_buffer.SetLength(0);

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
					_result.Values.Add(_charBuf[columnPos] != '"'
						// not quoted
						? new ArraySegment<char>(_charBuf, columnPos, (i - columnPos))
						// quoted
						: new ArraySegment<char>(_charBuf, columnPos + 1, (i - columnPos) - 2));
				}
				currentColumn++;
				columnPos = i+1;
			}
		}

		private void EnsureCharBuffer()
		{
			var maxCharCount = _encoding.GetMaxCharCount((int) _buffer.Length);
			if (maxCharCount > _charBuf.Length)
				_charBuf = new char[Utils.NearestPowerOfTwo(maxCharCount)];
		}
	}
}