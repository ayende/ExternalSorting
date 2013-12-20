using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
using Xunit;

namespace ExternalSorting.Tests
{
	public class ReadingText : IDisposable
	{
		private Stream _csv;

		public ReadingText()
		{
			_csv = Assembly.GetExecutingAssembly().GetManifestResourceStream("ExternalSorting.Tests.users.csv");
		}

		public void Dispose()
		{
			if (_csv != null)
				_csv.Dispose();
		}

		[Fact]
		public void CanReadFromStream()
		{
			var sourceReader = new SourceReader(_csv, Encoding.UTF8, 1);
			Assert.Equal(11, sourceReader.ReadFromStream().Count());
		}

		[Fact]
		public void CanReadQuotedValueFromStream()
		{
			var sourceReader = new SourceReader(_csv, Encoding.UTF8, 1);
			var arraySegment = sourceReader.ReadFromStream().Skip(2).First().Values[0];
			Assert.Equal("Darakjy", new string(arraySegment.Array, arraySegment.Offset, arraySegment.Count));
		}

		[Fact]
		public void CanReadUnQuotedValueFromStream()
		{
			var sourceReader = new SourceReader(_csv, Encoding.UTF8, 7);
			var arraySegment = sourceReader.ReadFromStream().Skip(8).First().Values[0];
			Assert.Equal("95111", new string(arraySegment.Array, arraySegment.Offset, arraySegment.Count));
		}
	}
}