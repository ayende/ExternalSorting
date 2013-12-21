using System;
using System.Collections.Generic;
using System.ComponentModel.Design;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;

namespace ExternalSorting.Tryouts
{
	class Program
	{
		static void Main(string[] args)
		{
			var options = new DirectoryExternalStorageOptions("indexing");
			var input = File.OpenRead(@"C:\Users\Ayende\Downloads\Crimes_-_2001_to_present.csv");
			//var sorter = new ExternalSorter(input, options, new int[]
			//{
			//	1,// case number
			//	4, // ICHR

			//});

			//var sp = Stopwatch.StartNew();

			//sorter.Sort();

			//Console.WriteLine(sp.Elapsed);

			//var r = new SourceReader(
			//	File.OpenRead(@"C:\work\ExternalSorting\ExternalSorting.Tryouts\bin\Debug\indexing\0.index"),
			//	Encoding.UTF8, new[] {0, 1});

			//r.SetPositionToLineAt(1000);
			//var result = r.ReadFromStream().First();

			//var prev = new IndexEntry
			//{
			//	Value = new ArraySegment<char>(new char[0])
			//};
			//int entries = 0;
			//for (int i = 0; i < r.NumberOfPages; i++)
			//{
			//	while (true)
			//	{
			//		var entry = r.Read();
			//		if (entry == null)
			//			break;
			//		entries++;
			//		var match = Utils.CompareIndexEntries(prev, entry);
			//		Console.WriteLine(new string(prev.Value.Array, prev.Value.Offset, prev.Value.Count) + " - " + new string(entry.Value.Array, entry.Value.Offset, entry.Value.Count) + " = " + match);
			//		var array = new char[entry.Value.Count];
			//		Array.Copy(entry.Value.Array, entry.Value.Offset, array, 0, entry.Value.Count);
			//		prev.Value = new ArraySegment<char>(array);
			//	}
			//}

			//Console.WriteLine();
			//Console.WriteLine(entries);

			var searcher = new IndexSearcher(input, File.OpenRead(@"C:\work\ExternalSorting\ExternalSorting.Tryouts\bin\Debug\indexing\0.index"),
				Encoding.UTF8);

			for (int i = 0; i < 10; i++)
			{
				var sp = Stopwatch.StartNew();
				foreach (var line in searcher.Search(@"HT574031"))
				{
					Console.WriteLine(line);
				}
				Console.WriteLine(sp.Elapsed);
			}
		}
	}
}
