using System;
using System.IO;
using System.Text;

namespace ExternalSorting
{
	/// <summary>
	/// This class writes to the output stream the values.
	/// It is assumed that the values are sorted (although that isn't actually required for what we are doing in this class, it is 
	/// important elsewhere)
	/// </summary>
	public class IndexBuilder : IDisposable
	{
		private readonly BinaryWriter _writer;
		private readonly char[] _buffer = new char[64];
		private static readonly char[] _lineBreak = { '\r', '\n' };

		public IndexBuilder(Stream output, Encoding encoding)
		{
			_writer = new BinaryWriter(output, encoding);
		}

		public void Add(IndexEntry entry)
		{
			for (int i = 0; i < entry.Value.Count; i++)
			{
				_writer.Write(entry.Value.Array[i + entry.Value.Offset]);
			}
			_writer.Write(',');
			var pos = entry.Position;
			int digits = 0;
			do
			{
				var remaining = (int) (pos%10);
				pos /= 10;
				_buffer[digits++] = "0123456789"[remaining];
			} while (pos > 0);

			Array.Reverse(_buffer,0,digits);

			_writer.Write(_buffer, 0, digits);
			
			_writer.Write(_lineBreak);
		}

		public void Dispose()
		{
			_writer.Flush();

			// explicitly do not dispose writer
		}
	}
}