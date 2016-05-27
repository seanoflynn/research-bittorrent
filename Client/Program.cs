using System;
using System.IO;
using Mono.Unix;
using Mono.Unix.Native;
using BitTorrent;

namespace Program
{
    public class Program
    {
        public static Client Client;

        public static void Main(string[] args)
        {
            int port = -1;

            if (args.Length != 3 || !Int32.TryParse(args[0], out port) || !File.Exists(args[1]))
            {
                Console.WriteLine("Error: requires port, torrent file and download directory as first, second and third arguments");
                return;
            }

            Client = new Client(port, args[1], args[2]);
            Client.Start();

            new UnixSignal(Signum.SIGINT).WaitOne();
            Client.Stop();
        }
    }
}

