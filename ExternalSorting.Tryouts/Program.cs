using System;
using System.Collections.Generic;
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
			var sorter = new ExternalSorter(input, options, new int[]
			{
				1,// case number
				4, // ICHR

			});

			var sp = Stopwatch.StartNew();

			sorter.Sort();

			Console.WriteLine(sp.Elapsed);
		}
	}
}
