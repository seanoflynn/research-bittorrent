using System;
using System.Linq;
using System.Text;
using System.Security.Cryptography;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Net;

namespace BitTorrent
{
    #region FileItem

    public class FileItem
    {            
        public string Path;
        public long Size;
        public long Offset;

        public string FormattedSize { get { return Torrent.BytesToString(Size); } }
    }

    #endregion

    public class Torrent
    {
        #region Events

        public event EventHandler<int> PieceVerified;
        public event EventHandler<List<IPEndPoint>> PeerListUpdated;

        #endregion

        #region Properties

        public string Name { get; private set; }
        public bool? IsPrivate { get; private set; }
        public List<FileItem> Files { get; private set; } = new List<FileItem>();
        public string FileDirectory { get { return (Files.Count > 1 ? Name + Path.DirectorySeparatorChar : ""); } }
        public string DownloadDirectory { get; private set; }

        public List<Tracker> Trackers { get; } = new List<Tracker>();
        public string Comment { get; set; }
        public string CreatedBy { get; set; }
        public DateTime CreationDate { get; set; }
        public Encoding Encoding { get; set; }

        public int BlockSize { get; private set; }
        public int PieceSize { get; private set; }
        public long TotalSize { get { return Files.Sum(x => x.Size); } }

        public string FormattedPieceSize { get { return BytesToString(PieceSize); } }
        public string FormattedTotalSize { get { return BytesToString(TotalSize); } }

        public int PieceCount { get { return PieceHashes.Length; } }

        public byte[][] PieceHashes { get; private set; }
        public bool[] IsPieceVerified { get; private set; }
        public bool[][] IsBlockAcquired { get; private set; }

        public string VerifiedPiecesString { get { return String.Join("", IsPieceVerified.Select(x => x ? 1 : 0)); } }
        public int VerifiedPieceCount { get { return IsPieceVerified.Count(x => x); } }
        public double VerifiedRatio { get { return VerifiedPieceCount / (double)PieceCount; } }
        public bool IsCompleted { get { return VerifiedPieceCount == PieceCount; } }
        public bool IsStarted { get { return VerifiedPieceCount > 0; } }

        public long Uploaded { get; set; } = 0;
        public long Downloaded { get { return PieceSize * VerifiedPieceCount; } } // !! incorrect
        public long Left { get { return TotalSize - Downloaded; } }

        public byte[] Infohash { get; private set; } = new byte[20];
        public string HexStringInfohash { get { return String.Join("", this.Infohash.Select(x => x.ToString("x2"))); } }
        public string UrlSafeStringInfohash { get { return Encoding.UTF8.GetString(WebUtility.UrlEncodeToBytes(this.Infohash, 0, 20)); } }

        #endregion

        #region Fields

        private object[] fileWriteLocks;
        private static SHA1 sha1 = SHA1.Create();

        #endregion

        #region Constructor

        public Torrent(string name, string location, List<FileItem> files, List<string> trackers, int pieceSize, byte[] pieceHashes = null, int blockSize = 16384, bool? isPrivate = false )
        {       
            Name = name;
            DownloadDirectory = location;
            Files = files;
            fileWriteLocks = new object[Files.Count];
            for (int i = 0; i < this.Files.Count; i++)
                fileWriteLocks[i] = new object();   

            if (trackers != null)
            {
                foreach (string url in trackers)
                {
                    Tracker tracker = new Tracker(url);
                    Trackers.Add(tracker);
                    tracker.PeerListUpdated += HandlePeerListUpdated;
                }
            }

            PieceSize = pieceSize;
            BlockSize = blockSize;
            IsPrivate = isPrivate;

            int count = Convert.ToInt32(Math.Ceiling(TotalSize / Convert.ToDouble(PieceSize)));

            PieceHashes = new byte[count][];
            IsPieceVerified = new bool[count];
            IsBlockAcquired = new bool[count][];

            for (int i = 0; i < PieceCount; i++)
                IsBlockAcquired[i] = new bool[GetBlockCount(i)];            

            if (pieceHashes == null)
            {
                // this is a new torrent so we have to create the hashes from the files                 
                for (int i = 0; i < PieceCount; i++)
                    PieceHashes[i] = GetHash(i);
            }
            else
            {
                for (int i = 0; i < PieceCount; i++)
                {
                    PieceHashes[i] = new byte[20];
                    Buffer.BlockCopy(pieceHashes, i * 20, PieceHashes[i], 0, 20);
                }
            }

            object info = TorrentInfoToBEncodingObject(this);
            byte[] bytes =  BEncoding.Encode(info);
            Infohash = SHA1.Create().ComputeHash(bytes);

            for (int i = 0; i < PieceCount; i++)
                Verify(i);
        }

        #endregion

        #region Creation

        public static Torrent Create(string path, List<string> trackers = null, int pieceSize = 32768, string comment = "")
        {
            string name = "";
            List<FileItem> files = new List<FileItem>();

            if (File.Exists(path))
            {
                name = Path.GetFileName(path);

                long size = new FileInfo(path).Length;
                files.Add(new FileItem()
                    {
                        Path = Path.GetFileName(path),
                        Size = size
                    });
            }
            else
            {
                name = path;
                string directory = path + Path.DirectorySeparatorChar;

                long running = 0;
                foreach (string file in Directory.EnumerateFiles(path, "*.*", SearchOption.AllDirectories))
                {
                    string f = file.Substring(directory.Length);

                    if (f.StartsWith("."))
                        continue;

                    long size = new FileInfo(file).Length;

                    files.Add(new FileItem()
                        {
                            Path = f,
                            Size = size,
                            Offset = running
                        });
                    
                    running += size;
                }
            }

            Torrent torrent = new Torrent(name, "", files, trackers, pieceSize);
            torrent.Comment = comment;
            torrent.CreatedBy = "TestClient";
            torrent.CreationDate = DateTime.Now;
            torrent.Encoding = Encoding.UTF8;

            return torrent;
        }

        #endregion

        #region Trackers

        public void UpdateTrackers(TrackerEvent ev, string id, int port)
        {
            foreach (var tracker in Trackers)
                tracker.Update(this, ev, id, port);
        }

        public void ResetTrackersLastRequest()
        {
            foreach (var tracker in Trackers)
                tracker.ResetLastRequest();
        }

        private void HandlePeerListUpdated(object sender, List<IPEndPoint> endPoints)
        {
            var handler = PeerListUpdated;
            if (handler != null)
                handler(sender, endPoints);
        }

        #endregion

        #region Verification

        public void Verify(int piece)
        {
            byte[] hash = GetHash(piece);

            bool isVerified = (hash != null && hash.SequenceEqual(PieceHashes[piece]));

            if (isVerified)
            {                
                IsPieceVerified[piece] = true;

                for (int j = 0; j < IsBlockAcquired[piece].Length; j++)
                    IsBlockAcquired[piece][j] = true;

                var handler = PieceVerified;
                if (handler != null)
                    handler(this, piece);

                return;
            }

            IsPieceVerified[piece] = false;

            // reload the entire piece
            if (IsBlockAcquired[piece].All(x => x))
            {
                for (int j = 0; j < IsBlockAcquired[piece].Length; j++)
                    IsBlockAcquired[piece][j] = false;
            }
        }

        public byte[] GetHash(int piece)
        {
            byte[] data = ReadPiece(piece);

            if (data == null)
                return null;            

            return sha1.ComputeHash(data);
        }

        #endregion

        #region File Read/Write

        public byte[] ReadPiece(int piece)
        {
            return Read(piece * PieceSize, GetPieceSize(piece));
        }

        public byte[] ReadBlock(int piece, int offset, int length)
        {
            return Read(piece * PieceSize + offset, length);
        }

        public byte[] Read(long start, int length)
        {
            long end = start + length;
            byte[] buffer = new byte[length];

            for (int i=0; i<Files.Count; i++)
            {                
                if ((start < Files[i].Offset && end < Files[i].Offset) ||
                    (start > Files[i].Offset + Files[i].Size && end > Files[i].Offset + Files[i].Size))
                    continue;

                string filePath = DownloadDirectory + Path.DirectorySeparatorChar + FileDirectory + Files[i].Path;

                if (!File.Exists(filePath))
                    return null;

                long fstart = Math.Max(0, start - Files[i].Offset);
                long fend = Math.Min(end - Files[i].Offset, Files[i].Size);
                int flength = Convert.ToInt32(fend - fstart);
                int bstart = Math.Max(0, Convert.ToInt32(Files[i].Offset - start));

                using (Stream stream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                {                    
                    stream.Seek(fstart, SeekOrigin.Begin);
                    stream.Read(buffer, bstart, flength);
                }
            }

            return buffer;
        }

        public void WriteBlock(int piece, int block, byte[] bytes)
        {            
            Write(piece * PieceSize + block * BlockSize, bytes);
            IsBlockAcquired[piece][block] = true;
            Verify(piece);
        }

        public void Write(long start, byte[] bytes)
        {
            long end = start + bytes.Length;

            for (int i = 0; i < Files.Count; i++)
            {                
                if ((start < Files[i].Offset && end < Files[i].Offset) ||
                    (start > Files[i].Offset + Files[i].Size && end > Files[i].Offset + Files[i].Size))
                    continue;

                string filePath = DownloadDirectory + Path.DirectorySeparatorChar + FileDirectory + Files[i].Path;

                string dir = Path.GetDirectoryName(filePath);
                if (!Directory.Exists(dir))
                    Directory.CreateDirectory(dir);

                lock (fileWriteLocks[i])
                {
                    using (Stream stream = new FileStream(filePath, FileMode.OpenOrCreate, FileAccess.ReadWrite))
                    {
                        long fstart = Math.Max(0, start - Files[i].Offset);
                        long fend = Math.Min(end - Files[i].Offset, Files[i].Size);
                        int flength = Convert.ToInt32(fend - fstart);
                        int bstart = Math.Max(0, Convert.ToInt32(Files[i].Offset - start));

                        stream.Seek(fstart, SeekOrigin.Begin);
                        stream.Write(bytes, bstart, flength);
                    }
                }
            }
        }

        #endregion

        #region File Import/Export

        public static Torrent LoadFromFile(string filePath, string downloadPath)
        {
            object obj = BEncoding.DecodeFile(filePath);
            string name = Path.GetFileNameWithoutExtension(filePath);

            return BEncodingObjectToTorrent(obj, name, downloadPath);
        }

        public static void SaveToFile(Torrent torrent)
        {
            object obj = TorrentToBEncodingObject(torrent);

            BEncoding.EncodeToFile(obj, torrent.Name + ".torrent");
        }

        #endregion

        #region BEncoding Import/Export

        private static object TorrentToBEncodingObject(Torrent torrent)
        {
            Dictionary<string,object> dict = new Dictionary<string, object>();

            if( torrent.Trackers.Count == 1 )
                dict["announce"] = Encoding.UTF8.GetBytes(torrent.Trackers[0].Address);
            else
                dict["announce"] = torrent.Trackers.Select(x => (object)Encoding.UTF8.GetBytes(x.Address)).ToList();
            dict["comment"] = Encoding.UTF8.GetBytes(torrent.Comment);
            dict["created by"] = Encoding.UTF8.GetBytes(torrent.CreatedBy);
            dict["creation date"] = DateTimeToUnixTimestamp(torrent.CreationDate);
            dict["encoding"] = Encoding.UTF8.GetBytes(Encoding.UTF8.WebName.ToUpper());
            dict["info"] = TorrentInfoToBEncodingObject(torrent);

            return dict;
        }

        private static object TorrentInfoToBEncodingObject(Torrent torrent)
        {
            Dictionary<string,object> dict = new Dictionary<string, object>();

            dict["piece length"] = (long)torrent.PieceSize;
            byte[] pieces = new byte[20 * torrent.PieceCount];
            for (int i = 0; i < torrent.PieceCount; i++)
                Buffer.BlockCopy(torrent.PieceHashes[i], 0, pieces, i * 20, 20);
            dict["pieces"] = pieces;

            if (torrent.IsPrivate.HasValue)
                dict["private"] = torrent.IsPrivate.Value ? 1L : 0L;

            if (torrent.Files.Count == 1)
            {                
                dict["name"] = Encoding.UTF8.GetBytes(torrent.Files[0].Path);
                dict["length"] = torrent.Files[0].Size;
            }
            else
            {
                List<object> files = new List<object>();

                foreach (var f in torrent.Files)
                {
                    Dictionary<string,object> fileDict = new Dictionary<string, object>();
                    fileDict["path"] = f.Path.Split(Path.DirectorySeparatorChar).Select(x => (object)Encoding.UTF8.GetBytes(x)).ToList();
                    fileDict["length"] = f.Size;
                    files.Add(fileDict);
                }

                dict["files"] = files;
                dict["name"] = Encoding.UTF8.GetBytes(torrent.FileDirectory.Substring(0, torrent.FileDirectory.Length - 1));
            }

            return dict;
        }

        private static Torrent BEncodingObjectToTorrent(object bencoding, string name, string downloadPath)
        {
            Dictionary<string,object> obj = (Dictionary<string,object>)bencoding;

            if (obj == null)
                throw new Exception("not a torrent file");
            
            // !! handle list
            List<string> trackers = new List<string>();
            if (obj.ContainsKey("announce"))                
                trackers.Add(DecodeUTF8String(obj["announce"]));            

            if (!obj.ContainsKey("info"))
                throw new Exception("Missing info section");

            Dictionary<string,object> info = (Dictionary<string,object>)obj["info"];

            if (info == null)
                throw new Exception("error");

            List<FileItem> files = new List<FileItem>();

            if (info.ContainsKey("name") && info.ContainsKey("length"))
            {
                files.Add(new FileItem() {
                    Path = DecodeUTF8String(info["name"]),
                    Size = (long)info["length"]
                });
            }
            else if (info.ContainsKey("files"))
            {
                long running = 0;

                foreach (object item in (List<object>)info["files"])
                {
                    var dict = item as Dictionary<string,object>;

                    if (dict == null || !dict.ContainsKey("path") || !dict.ContainsKey("length") )
                        throw new Exception("error: incorrect file specification");

                    string path = String.Join(Path.DirectorySeparatorChar.ToString(), ((List<object>)dict["path"]).Select(x => DecodeUTF8String(x)));

                    long size = (long)dict["length"];

                    files.Add(new FileItem() {
                        Path = path,
                        Size = size,
                        Offset = running
                    });

                    running += size;
                }
            }
            else
            {
                throw new Exception("error: no files specified in torrent");
            }
                
            if (!info.ContainsKey("piece length"))
                throw new Exception("error");
            int pieceSize = Convert.ToInt32(info["piece length"]);

            if (!info.ContainsKey("pieces"))
                throw new Exception("error");            
            byte[] pieceHashes = (byte[])info["pieces"];

            bool? isPrivate = null;
            if (info.ContainsKey("private"))
                isPrivate = ((long)info["private"]) == 1L;            
            
            Torrent torrent = new Torrent(name, downloadPath, files, trackers, pieceSize, pieceHashes, 16384, isPrivate );

            if (obj.ContainsKey("comment"))
                torrent.Comment = DecodeUTF8String(obj["comment"]);

            if (obj.ContainsKey("created by"))
                torrent.CreatedBy = DecodeUTF8String(obj["created by"]);

            if (obj.ContainsKey("creation date"))
                torrent.CreationDate = UnixTimeStampToDateTime(Convert.ToDouble(obj["creation date"]));

            if (obj.ContainsKey("encoding"))
                torrent.Encoding = Encoding.GetEncoding(DecodeUTF8String(obj["encoding"]));
            
            return torrent;
        }

        #endregion

        #region Size Helpers

        public int GetPieceSize(int piece)
        {            
            if (piece == PieceCount - 1)
            { 
                int remainder = Convert.ToInt32(TotalSize % PieceSize);
                if (remainder != 0)
                    return remainder;
            }

            return PieceSize;
        }

        public int GetBlockSize(int piece, int block)
        {
            if (block == GetBlockCount(piece) - 1)
            {
                int remainder = Convert.ToInt32(GetPieceSize(piece) % BlockSize);
                if (remainder != 0)
                    return remainder;
            }

            return BlockSize;
        }

        public int GetBlockCount(int piece)
        {
            return Convert.ToInt32(Math.Ceiling(GetPieceSize(piece) / (double)BlockSize));
        }

        #endregion

        #region Helpers

        public static string DecodeUTF8String( object obj )
        {
            byte[] bytes = obj as byte[];

            if (bytes == null)
                throw new Exception("unable to decode utf-8 string, object is not a byte array");

            return Encoding.UTF8.GetString(bytes);
        }

        public static DateTime UnixTimeStampToDateTime( double unixTimeStamp )
        {
            // Unix timestamp is seconds past epoch
            System.DateTime dtDateTime = new DateTime(1970,1,1,0,0,0,0,System.DateTimeKind.Utc);
            dtDateTime = dtDateTime.AddSeconds( unixTimeStamp ).ToLocalTime();
            return dtDateTime;
        }

        public static long DateTimeToUnixTimestamp( DateTime time )
        {
            return Convert.ToInt64((DateTime.UtcNow.Subtract(new DateTime(1970, 1, 1))).TotalSeconds);
        }

        // deepeer1 & Erik Schierboom (http://stackoverflow.com/a/4975942)
        public static string BytesToString(long bytes)
        {
            string[] units = { "B", "KB", "MB", "GB", "TB", "PB", "EB" };
            if (bytes == 0)
                return "0" + units[0];
            int place = Convert.ToInt32(Math.Floor(Math.Log(bytes, 1024)));
            double num = Math.Round(bytes / Math.Pow(1024, place), 1);
            return num + units[place];
        }

        public override string ToString()
        {
            return string.Format("[Torrent: {0} {1} ({2}x{3}) Verified {4}/{5} ({6:P1}) {7}]", Name, FormattedTotalSize, PieceCount, FormattedPieceSize, VerifiedPieceCount, PieceCount, VerifiedRatio, HexStringInfohash);
        }

        public string ToDetailedString()
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine("Torrent: ".PadRight(15) + Name);
            sb.AppendLine("Size: ".PadRight(15) + FormattedTotalSize);
            sb.AppendLine("Pieces: ".PadRight(15) + PieceCount + "x" + FormattedPieceSize);
            sb.AppendLine("Verified: ".PadRight(15) + VerifiedPieceCount + "/" + PieceCount + " (" + VerifiedRatio.ToString("P1") + ")");
            sb.AppendLine("Hash: ".PadRight(15) + HexStringInfohash);
            sb.AppendLine("Created By: ".PadRight(15) + CreatedBy);
            sb.AppendLine("Creation Date: ".PadRight(15) + CreationDate);
            sb.AppendLine("Comment: ".PadRight(15) + Comment);
            if (Files.Count == 1)
                sb.Append("File: ".PadRight(15) + Files[0].Path + " (" + Files[0].FormattedSize + ")");
            else
            {
                sb.Append("Files: ".PadRight(15) + Files.Count);
                for (int i = 0; i < Files.Count; i++)
                    sb.Append("\n" + ("- " + (i + 1) + ":").PadRight(15) + FileDirectory + Files[i].Path + " (" + Files[i].FormattedSize + ")");
            }

            return sb.ToString();
        }

        #endregion
    }
}