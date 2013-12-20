using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;

namespace ExternalSorting
{
	public abstract class ExternalStorageOptions
	{
		public long MaxHeldMemory = 1024 * 1024 * 64;
		public Encoding Encoding = Encoding.UTF8;

		public abstract Stream Create(int index, long counter);

		public abstract List<Stream> GetAllFor(int index);
	}

	public class DirectoryExternalStorageOptions : ExternalStorageOptions
	{
		private readonly string _basePath;

		public DirectoryExternalStorageOptions(string basePath)
		{
			_basePath = basePath;
			if (Directory.Exists(_basePath) == false)
				Directory.CreateDirectory(_basePath);
		}

		public override Stream Create(int index, long counter)
		{
			var path = Path.Combine(_basePath, index.ToString(CultureInfo.InvariantCulture));
			if (Directory.Exists(path) == false)
				Directory.CreateDirectory(path);
			return File.Create(Path.Combine(path, counter + ".index-part"));
		}

		public override List<Stream> GetAllFor(int index)
		{
			return Directory.GetFiles(Path.Combine(_basePath, index.ToString(CultureInfo.InvariantCulture)), "*.index-part")
				.Select(file => (Stream)File.OpenRead(file))
				.ToList();
		}
	}
}