using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Linq;
using System.Threading;

namespace BitTorrent
{
    public class Client
    {
        #region Parameters

        public int Port { get; private set; }
        public Torrent Torrent { get; private set; }

        private static int maxLeechers = 5;
        private static int maxSeeders = 5;

        private static int maxUploadBytesPerSecond = 16384;
        private static int maxDownloadBytesPerSecond = 16384;

        private static TimeSpan peerTimeout = TimeSpan.FromSeconds(30);

        #endregion

        public string Id { get; private set; }

        private TcpListener listener;

        public ConcurrentDictionary<string,Peer> Peers { get; } = new ConcurrentDictionary<string,Peer>();
        public ConcurrentDictionary<string,Peer> Seeders { get; } = new ConcurrentDictionary<string,Peer>();
        public ConcurrentDictionary<string,Peer> Leechers { get; } = new ConcurrentDictionary<string,Peer>();

        private bool isStopping;
        private int isProcessPeers = 0;
        private int isProcessUploads = 0;
        private int isProcessDownloads = 0;

        private ConcurrentQueue<DataRequest> OutgoingBlocks = new ConcurrentQueue<DataRequest>();
        private ConcurrentQueue<DataPackage> IncomingBlocks = new ConcurrentQueue<DataPackage>();

        private Throttle uploadThrottle = new Throttle(maxUploadBytesPerSecond, TimeSpan.FromSeconds(1));
        private Throttle downloadThrottle = new Throttle(maxDownloadBytesPerSecond, TimeSpan.FromSeconds(1));

        private Random random = new Random();

        public Client(int port, string torrentPath, string downloadPath)
        {
            // generate random numerical id
            Id = "";
            for (int i = 0; i < 20; i++)
                Id += (random.Next(0, 10));

            Port = port;

            Torrent = Torrent.LoadFromFile(torrentPath, downloadPath);
            Torrent.PieceVerified += HandlePieceVerified;
            Torrent.PeerListUpdated += HandlePeerListUpdated;

            Log.WriteLine(Torrent);
        }

        #region Management

        public void Start()
        {
            Log.WriteLine("starting client");

            isStopping = false;

            Torrent.ResetTrackersLastRequest();

            EnablePeerConnections();

            // tracker thread
            new Thread(new ThreadStart(() =>
                    {
                        while (!isStopping)
                        {
                            Torrent.UpdateTrackers(TrackerEvent.Started, Id, Port);
                            Thread.Sleep(10000);
                        }
                    })).Start();

            // peer thread
            new Thread(new ThreadStart(() =>
                    { 
                        while (!isStopping)
                        {
                            ProcessPeers();
                            Thread.Sleep(1000);
                        }
                    })).Start();

            // upload thread
            new Thread(new ThreadStart(() =>
                    { 
                        while (!isStopping)
                        {
                            ProcessUploads();
                            Thread.Sleep(1000);
                        }
                    })).Start();

            // download thread
            new Thread(new ThreadStart(() =>
                    { 
                        while (!isStopping)
                        {
                            ProcessDownloads();
                            Thread.Sleep(1000);
                        }
                    })).Start();
        }

        public void Stop()
        {
            Log.WriteLine("stopping client");

            isStopping = true;
            DisablePeerConnections();
            Torrent.UpdateTrackers(TrackerEvent.Stopped, Id, Port);
        }

        #endregion

        #region Peers

        private static IPAddress LocalIPAddress
        {
            get
            {
                var host = Dns.GetHostEntry(Dns.GetHostName());
                foreach (var ip in host.AddressList)
                {
                    if (ip.AddressFamily == AddressFamily.InterNetwork)
                        return ip;                
                }
                throw new Exception("Local IP Address Not Found!");
            }
        }

        private void HandlePeerListUpdated(object sender, List<IPEndPoint> endPoints)
        {
            IPAddress local = LocalIPAddress;

            foreach(var endPoint in endPoints)
            {
                if (endPoint.Address.Equals(local) && endPoint.Port == Port)
                    continue;
                
                AddPeer(new Peer(Torrent, Id, endPoint));
            }

            Log.WriteLine("received peer information from " + (Tracker)sender);
            Log.WriteLine("peer count: " + Peers.Count);
        }

        private void EnablePeerConnections()
        {
            listener = new TcpListener(new IPEndPoint(IPAddress.Any, Port));
            listener.Start();
            listener.BeginAcceptTcpClient(new AsyncCallback(HandleNewConnection), null);

            Log.WriteLine("started listening for incoming peer connections on port " + Port);
        }

        private void HandleNewConnection(IAsyncResult ar)
        {
            if (listener == null)
                return;

            TcpClient client = listener.EndAcceptTcpClient(ar);
            listener.BeginAcceptTcpClient(new AsyncCallback(HandleNewConnection), null);

            AddPeer(new Peer(Torrent, Id, client));
        }

        private void DisablePeerConnections()
        {
            listener.Stop();
            listener = null;

            foreach (var peer in Peers)
                peer.Value.Disconnect();

            Log.WriteLine("stopped listening for incoming peer connections on port " + Port);
        }

        private void AddPeer(Peer peer)
        {
            peer.BlockRequested += HandleBlockRequested;
            peer.BlockCancelled += HandleBlockCancelled;
            peer.BlockReceived += HandleBlockReceived;
            peer.Disconnected += HandlePeerDisconnected;
            peer.StateChanged += HandlePeerStateChanged;

            peer.Connect();

            if (!Peers.TryAdd(peer.Key, peer))
                peer.Disconnect();
        }

        private void HandlePeerDisconnected(object sender, EventArgs args)
        {            
            Peer peer = sender as Peer;

            peer.BlockRequested -= HandleBlockRequested;
            peer.BlockCancelled -= HandleBlockCancelled;
            peer.BlockReceived -= HandleBlockReceived;
            peer.Disconnected -= HandlePeerDisconnected;
            peer.StateChanged -= HandlePeerStateChanged;

            Peer tmp;
            Peers.TryRemove(peer.Key, out tmp);
            Seeders.TryRemove(peer.Key, out tmp);
            Leechers.TryRemove(peer.Key, out tmp);
        }

        private void HandlePeerStateChanged(object sender, EventArgs args)
        {
            ProcessPeers();
        }

        private void HandlePieceVerified (object sender, int index)
        {
            ProcessPeers();

            foreach (var peer in Peers)
            {
                if (!peer.Value.IsHandshakeReceived || !peer.Value.IsHandshakeSent)
                    continue;

                peer.Value.SendHave(index);
            }
        }

        private void ProcessPeers()
        {
            if (Interlocked.Exchange(ref isProcessPeers, 1) == 1)
                return;

            foreach (var peer in Peers.OrderByDescending(x => x.Value.PiecesRequiredAvailable))
            {
                if (DateTime.UtcNow > peer.Value.LastActive.Add(peerTimeout))
                {                    
                    peer.Value.Disconnect();
                    continue;
                }

                if (!peer.Value.IsHandshakeSent || !peer.Value.IsHandshakeReceived)
                    continue;

                if (Torrent.IsCompleted)
                    peer.Value.SendNotInterested();
                else
                    peer.Value.SendInterested();

                if (peer.Value.IsCompleted && Torrent.IsCompleted)
                {
                    peer.Value.Disconnect();
                    continue;
                }

                peer.Value.SendKeepAlive();

                // let them leech
                if (Torrent.IsStarted && Leechers.Count < maxLeechers)
                {
                    if (peer.Value.IsInterestedReceived && peer.Value.IsChokeSent)
                        peer.Value.SendUnchoke();                
                }

                // ask to leech
                if (!Torrent.IsCompleted && Seeders.Count <= maxSeeders)
                {
                    if(!peer.Value.IsChokeReceived )
                        Seeders.TryAdd(peer.Key, peer.Value);
                }
            }

            Interlocked.Exchange(ref isProcessPeers, 0);
        }


        #endregion

        #region Uploads

        private void HandleBlockRequested(object sender, DataRequest block)
        {
            OutgoingBlocks.Enqueue(block);

            ProcessUploads();
        }

        private void HandleBlockCancelled(object sender, DataRequest block)
        {
            foreach (var item in OutgoingBlocks)
            {
                if (item.Peer != block.Peer || item.Piece != block.Piece || item.Begin != block.Begin || item.Length != block.Length)
                    continue;

                item.IsCancelled = true;
            }

            ProcessUploads();
        }

        private void ProcessUploads()
        {
            if (Interlocked.Exchange(ref isProcessUploads, 1) == 1)
                return;

            DataRequest block;
            while (!uploadThrottle.IsThrottled && OutgoingBlocks.TryDequeue(out block))
            {
                if (block.IsCancelled)
                    continue;

                if (!Torrent.IsPieceVerified[block.Piece])
                    continue;            

                byte[] data = Torrent.ReadBlock(block.Piece, block.Begin, block.Length);
                if (data == null)
                    continue;

                block.Peer.SendPiece(block.Piece, block.Begin, data);
                uploadThrottle.Add(block.Length);
                Torrent.Uploaded += block.Length;
            }

            Interlocked.Exchange(ref isProcessUploads, 0);
        }

        #endregion

        #region Downloads

        private void HandleBlockReceived(object sender, DataPackage args)
        {
            IncomingBlocks.Enqueue(args);

            args.Peer.IsBlockRequested[args.Piece][args.Block] = false;

            foreach(var peer in Peers)
            {
                if (!peer.Value.IsBlockRequested[args.Piece][args.Block])
                    continue;

                peer.Value.SendCancel(args.Piece, args.Block * Torrent.BlockSize, Torrent.BlockSize);
                peer.Value.IsBlockRequested[args.Piece][args.Block] = false;
            }

            ProcessDownloads();
        }

        private void ProcessDownloads()
        {
            if (Interlocked.Exchange(ref isProcessDownloads, 1) == 1)
                return;

            DataPackage incomingBlock;
            while (IncomingBlocks.TryDequeue(out incomingBlock))
                Torrent.WriteBlock(incomingBlock.Piece, incomingBlock.Block, incomingBlock.Data);

            if (Torrent.IsCompleted)
            {
                Interlocked.Exchange(ref isProcessDownloads, 0);
                return;
            }

            int[] ranked = GetRankedPieces();

            foreach (var piece in ranked)
            {
                if (Torrent.IsPieceVerified[piece])
                    continue;

                foreach (var peer in GetRankedSeeders())
                {
                    if (!peer.IsPieceDownloaded[piece])
                        continue;

                    // just request blocks in order
                    for (int block = 0; block < Torrent.GetBlockCount(piece); block++)
                    {                        
                        if (downloadThrottle.IsThrottled)
                            continue;

                        if(Torrent.IsBlockAcquired[piece][block])
                            continue;

                        // only request one block from each peer at a time
                        if (peer.BlocksRequested > 0)
                            continue;

                        // only request from 1 peer at a time
                        if (Peers.Count(x => x.Value.IsBlockRequested[piece][block]) > 0)
                            continue;

                        int size = Torrent.GetBlockSize(piece, block);
                        peer.SendRequest(piece, block * Torrent.BlockSize, size);
                        downloadThrottle.Add(size);
                        peer.IsBlockRequested[piece][block] = true;
                    }
                }
            }

            Interlocked.Exchange(ref isProcessDownloads, 0);
        }

        #endregion

        #region Ranking

        private int[] GetRankedPieces()
        {
            var indexes = Enumerable.Range(0, Torrent.PieceCount).ToArray();
            var scores = indexes.Select(x => GetPieceScore(x)).ToArray();
                
            Array.Sort(scores, indexes);
            Array.Reverse(indexes);

            return indexes;
        }

        private double GetPieceScore(int piece)
        {
            double progress = GetPieceProgress(piece);
            double rarity = GetPieceRarity(piece);

            if( progress == 1.0 )
                progress = 0;

            double rand = random.Next(0,100) / 1000.0;

            return progress + rarity + rand;
        }

        private double GetPieceProgress(int index)
        {
            return Torrent.IsBlockAcquired[index].Average(x => x ? 1.0 : 0.0);
        }

        private double GetPieceRarity(int index)
        {
            if(Peers.Count < 1 )
                return 0.0;

            return Peers.Average(x => x.Value.IsPieceDownloaded[index] ? 0.0 : 1.0);
        }

        private Peer[] GetRankedSeeders()
        {
            return Seeders.Values.OrderBy(x => random.Next(0, 100)).ToArray();
        }

        #endregion
    }
}

