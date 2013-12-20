using System;
using System.Collections.Generic;
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
			var input = File.OpenRead(@"c:\work\ExternalSorting\ExternalSorting.Tests\users.csv");
			var sorter = new ExternalSorter(input, options, 7, 10);
			sorter.Sort();
		}
	}
}
