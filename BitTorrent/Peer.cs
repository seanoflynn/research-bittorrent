using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Linq;
using System.Text;
using System.IO;
using MiscUtil.Conversion;

namespace BitTorrent
{
    public class DataRequest
    {
        public Peer Peer;
        public int Piece;
        public int Begin;
        public int Length;
        public bool IsCancelled;
    }

    public class DataPackage
    {
        public Peer Peer;
        public int Piece;
        public int Block;
        public byte[] Data;
    }

    public enum MessageType : int
    {
        Unknown = -3,
        Handshake = -2,
        KeepAlive = -1,
        Choke = 0,
        Unchoke = 1,
        Interested = 2,
        NotInterested = 3,
        Have = 4,
        Bitfield = 5,
        Request = 6,
        Piece = 7,
        Cancel = 8,
        Port = 9,
    }

    public class Peer
    {
        #region Events

        public event EventHandler Disconnected;
        public event EventHandler StateChanged;
        public event EventHandler<DataRequest> BlockRequested;
        public event EventHandler<DataRequest> BlockCancelled;
        public event EventHandler<DataPackage> BlockReceived;

        #endregion

        #region Properties

        public string LocalId { get; set; }
        public string Id { get; set; }

        public Torrent Torrent { get; private set; }

        public IPEndPoint IPEndPoint { get; private set; }
        public string Key { get { return IPEndPoint.ToString(); } }

        private TcpClient TcpClient { get; set; }
        private NetworkStream stream { get; set; }
        private const int bufferSize = 256;
        private byte[] streamBuffer = new byte[bufferSize];
        private List<byte> data = new List<byte>();

        public bool[] IsPieceDownloaded = new bool[0];
        public string PiecesDownloaded { get { return String.Join("", IsPieceDownloaded.Select(x => Convert.ToInt32(x))); } }
        public int PiecesRequiredAvailable { get { return IsPieceDownloaded.Select((x, i) => x && !Torrent.IsPieceVerified[i]).Count(x => x); } }
        public int PiecesDownloadedCount { get { return IsPieceDownloaded.Count(x => x); } }
        public bool IsCompleted { get { return PiecesDownloadedCount == Torrent.PieceCount; } }

        public bool IsDisconnected;

        public bool IsHandshakeSent;
        public bool IsPositionSent;
        public bool IsChokeSent = true;
        public bool IsInterestedSent = false;

        public bool IsHandshakeReceived;
        public bool IsChokeReceived = true;
        public bool IsInterestedReceived = false;

        public bool[][] IsBlockRequested = new bool[0][];
        public int BlocksRequested { get { return IsBlockRequested.Sum(x => x.Count(y => y)); } }

        public DateTime LastActive;
        public DateTime LastKeepAlive = DateTime.MinValue;

        public long Uploaded;
        public long Downloaded;

        #endregion

        #region Constructors

        public Peer(Torrent torrent, string localId, TcpClient client) : this(torrent, localId)
        {            
            TcpClient = client;
            IPEndPoint = (IPEndPoint)client.Client.RemoteEndPoint;
        }

        public Peer(Torrent torrent, string localId, IPEndPoint endPoint): this(torrent, localId)
        {
            IPEndPoint = endPoint;
        }

        private Peer(Torrent torrent, string localId)
        {
            LocalId = localId;
            Torrent = torrent;

            LastActive = DateTime.UtcNow;
            IsPieceDownloaded = new bool[Torrent.PieceCount];
            IsBlockRequested = new bool[Torrent.PieceCount][];
            for (int i = 0; i < Torrent.PieceCount; i++)
                IsBlockRequested[i] = new bool[Torrent.GetBlockCount(i)];
        }

        #endregion

        #region Tcp

        public void Connect()
        {
            if (TcpClient == null)
            {
                TcpClient = new TcpClient();
                try
                {
                    TcpClient.Connect(IPEndPoint);
                }
                catch (Exception e)
                {
                    Disconnect();
                    return;
                }
            }

            Log.WriteLine(this, "connected");

            stream = TcpClient.GetStream();
            stream.BeginRead(streamBuffer, 0, Peer.bufferSize, new AsyncCallback(HandleRead), null);

            SendHandshake();
            if (IsHandshakeReceived)
                SendBitfield(Torrent.IsPieceVerified);
        }

        public void Disconnect()
        {
            if (!IsDisconnected)
            {
                IsDisconnected = true;
                Log.WriteLine(this, "disconnected, down " + Downloaded + ", up " + Uploaded);
            }

            if (TcpClient != null)
                TcpClient.Close();

            if (Disconnected != null)
                Disconnected(this, new EventArgs());
        }
            
        private void SendBytes(byte[] bytes)
        {
            try
            {
                stream.Write(bytes, 0, bytes.Length);
            }
            catch(Exception e)
            {
                Disconnect();
            }
        }

        private void HandleRead( IAsyncResult ar )
        {            
            int bytes = 0;
            try
            {
                bytes = stream.EndRead(ar);
            }
            catch (Exception e)
            {
                Disconnect();
                return;
            }
               
            data.AddRange(streamBuffer.Take(bytes));

            int messageLength = GetMessageLength(data);
            while (data.Count >= messageLength)
            {
                HandleMessage(data.Take(messageLength).ToArray());
                data = data.Skip(messageLength).ToList();

                messageLength = GetMessageLength(data);
            }

            try
            {
                stream.BeginRead(streamBuffer, 0, Peer.bufferSize, new AsyncCallback(HandleRead), null);
            }
            catch (Exception e)
            {
                Disconnect();
            }
        }

        private int GetMessageLength(List<byte> data)
        {
            if (!IsHandshakeReceived)
                return 68;

            if (data.Count < 4)
                return int.MaxValue;

            return EndianBitConverter.Big.ToInt32(data.ToArray(), 0) + 4;
        }

        #endregion

        #region Incoming Messages

        private MessageType GetMessageType(byte[] bytes)
        {
            if (!IsHandshakeReceived)
                return MessageType.Handshake;

            if (bytes.Length == 4 && EndianBitConverter.Big.ToInt32(bytes, 0) == 0)
                return MessageType.KeepAlive;

            if (bytes.Length > 4 && Enum.IsDefined(typeof(MessageType), (int)bytes[4]))
                return (MessageType)bytes[4];

            return MessageType.Unknown;
        }

        private void HandleMessage(byte[] bytes)
        {
            LastActive = DateTime.UtcNow;

            MessageType type = GetMessageType(bytes);

            if (type == MessageType.Unknown)
            {
                return;   
            }
            else if (type == MessageType.Handshake)
            {
                byte[] hash;
                string id;
                if (DecodeHandshake(bytes, out hash, out id))
                {
                    HandleHandshake(hash, id);
                    return;
                }
            }
            else if (type == MessageType.KeepAlive && DecodeKeepAlive(bytes))
            {
                HandleKeepAlive();
                return;
            }
            else if (type == MessageType.Choke && DecodeChoke(bytes))
            {
                HandleChoke();
                return;
            }
            else if (type == MessageType.Unchoke && DecodeUnchoke(bytes))
            {
                HandleUnchoke();
                return;
            }
            else if (type == MessageType.Interested && DecodeInterested(bytes))
            {                
                HandleInterested();
                return;
            }
            else if (type == MessageType.NotInterested && DecodeNotInterested(bytes))
            {
                HandleNotInterested();
                return;
            }
            else if (type == MessageType.Have)
            {
                int index;
                if (DecodeHave(bytes, out index))
                {
                    HandleHave(index);
                    return;
                }                
            }
            else if (type == MessageType.Bitfield)
            {
                bool[] isPieceDownloaded;
                if (DecodeBitfield(bytes, IsPieceDownloaded.Length, out isPieceDownloaded))
                {
                    HandleBitfield(isPieceDownloaded);
                    return;
                }
            }
            else if (type == MessageType.Request)
            {
                int index;
                int begin;
                int length;
                if (DecodeRequest(bytes, out index, out begin, out length))
                {
                    HandleRequest(index, begin, length);
                    return;
                }
            }
            else if (type == MessageType.Piece)
            {
                int index;
                int begin;
                byte[] data;
                if (DecodePiece(bytes, out index, out begin, out data))
                {
                    HandlePiece(index, begin, data);
                    return;
                }
            }
            else if (type == MessageType.Cancel)
            {
                int index;
                int begin;
                int length;
                if (DecodeCancel(bytes, out index, out begin, out length))
                {
                    HandleCancel(index, begin, length);
                    return;
                }
            }
            else if (type == MessageType.Port)
            {
                Log.WriteLine(this, " <- port: " + String.Join("", bytes.Select(x => x.ToString("x2"))));
                return;
            }

            Log.WriteLine(this, " Unhandled incoming message " + String.Join("", bytes.Select(x => x.ToString("x2"))));
            Disconnect();
        }

        private void HandleHandshake(byte[] hash, string id)
        {
            Log.WriteLine(this, "<- handshake");

            if (!Torrent.Infohash.SequenceEqual(hash))
            {
                Log.WriteLine(this, "invalid handshake, incorrect torrent hash: expecting=" + Torrent.HexStringInfohash + ", received =" + String.Join("", hash.Select(x => x.ToString("x2"))));
                Disconnect();
                return;
            }

            Id = id;

            IsHandshakeReceived = true;
            SendBitfield(Torrent.IsPieceVerified);
        }            

        private void HandleKeepAlive() 
        {
            Log.WriteLine(this, "<- keep alive");
        }

        private void HandleChoke() 
        {
            Log.WriteLine(this, "<- choke");
            IsChokeReceived = true;

            var handler = StateChanged;
            if (handler != null)
                handler(this, new EventArgs());
        }

        private void HandleUnchoke() 
        {
            Log.WriteLine(this, "<- unchoke");
            IsChokeReceived = false;

            var handler = StateChanged;
            if (handler != null)
                handler(this, new EventArgs());
        }

        private void HandleInterested() 
        {
            Log.WriteLine(this, "<- interested");
            IsInterestedReceived = true;

            var handler = StateChanged;
            if (handler != null)
                handler(this, new EventArgs());
        }

        private void HandleNotInterested() 
        {
            Log.WriteLine(this, "<- not interested");
            IsInterestedReceived = false;

            var handler = StateChanged;
            if (handler != null)
                handler(this, new EventArgs());
        }

        private void HandleHave(int index)
        {
            IsPieceDownloaded[index] = true;
            Log.WriteLine(this, "<- have " + index + " - " + PiecesDownloadedCount + " available (" + PiecesDownloaded + ")");

            var handler = StateChanged;
            if (handler != null)
                handler(this, new EventArgs());
        }

        private void HandleBitfield(bool[] isPieceDownloaded)
        {
            for (int i = 0; i < Torrent.PieceCount; i++)
                IsPieceDownloaded[i] = IsPieceDownloaded[i] || isPieceDownloaded[i];

            Log.WriteLine(this, "<- bitfield " + PiecesDownloadedCount + " available (" + PiecesDownloaded + ")");

            var handler = StateChanged;
            if (handler != null)
                handler(this, new EventArgs());
        }

        private void HandleRequest(int index, int begin, int length)
        {
            Log.WriteLine(this, "<- request " + index + ", " + begin + ", " + length);

            var handler = BlockRequested;
            if (handler != null)
            {
                handler(this, new DataRequest()
                    {
                        Peer = this,
                        Piece = index,
                        Begin = begin,
                        Length = length
                    });
            }
        }

        private void HandlePiece(int index, int begin, byte[] data) 
        {
            Log.WriteLine(this, "<- piece " + index + ", " + begin + ", " + data.Length);
            Downloaded += data.Length;

            var handler = BlockReceived;
            if (handler != null)
            {
                handler(this, new DataPackage()
                    {
                        Peer = this,
                        Piece = index,
                        Block = begin / Torrent.BlockSize,
                        Data = data
                    });
            }
        }

        private void HandleCancel(int index, int begin, int length) 
        {
            Log.WriteLine(this, " <- cancel");

            var handler = BlockCancelled;
            if (handler != null)
            {
                handler(this, new DataRequest()
                    {
                        Peer = this,
                        Piece = index,
                        Begin = begin,
                        Length = length
                    });
            }
        }

        private void HandlePort(int port) 
        {
            Log.WriteLine(this, "<- port");
        }

        #endregion

        #region Outgoing Messages

        private void SendHandshake()
        {
            if (IsHandshakeSent)
                return;

            Log.WriteLine(this, "-> handshake" );
            SendBytes(EncodeHandshake(Torrent.Infohash, LocalId));
            IsHandshakeSent = true;
        }

        public void SendKeepAlive()
        {
            if( LastKeepAlive > DateTime.UtcNow.AddSeconds(-30) )
                return;

            Log.WriteLine(this, "-> keep alive" );
            SendBytes(EncodeKeepAlive());
            LastKeepAlive = DateTime.UtcNow;
        }

        public void SendChoke() 
        {
            if (IsChokeSent)
                return;
            
            Log.WriteLine(this, "-> choke" );
            SendBytes(EncodeChoke());
            IsChokeSent = true;
        }

        public void SendUnchoke() 
        {
            if (!IsChokeSent)
                return;
            
            Log.WriteLine(this, "-> unchoke" );
            SendBytes(EncodeUnchoke());
            IsChokeSent = false;
        }

        public void SendInterested()
        {
            if (IsInterestedSent)
                return;
            
            Log.WriteLine(this, "-> interested");
            SendBytes(EncodeInterested());
            IsInterestedSent = true;
        }

        public void SendNotInterested() 
        {
            if (!IsInterestedSent)
                return;

            Log.WriteLine(this, "-> not interested");
            SendBytes(EncodeNotInterested());
            IsInterestedSent = false;
        }

        public void SendHave(int index) 
        {
            Log.WriteLine(this, "-> have " + index);
            SendBytes(EncodeHave(index));
        }

        public void SendBitfield(bool[] isPieceDownloaded) 
        {
            Log.WriteLine(this, "-> bitfield " + String.Join("", isPieceDownloaded.Select(x => x ? 1 : 0)));
            SendBytes(EncodeBitfield(isPieceDownloaded));
        }

        public void SendRequest(int index, int begin, int length) 
        {
            Log.WriteLine(this, "-> request " + index + ", " + begin + ", " + length);
            SendBytes(EncodeRequest(index, begin, length));
        }

        public void SendPiece(int index, int begin, byte[] data) 
        {
            Log.WriteLine(this, "-> piece " + index + ", " + begin + ", " + data.Length);
            SendBytes(EncodePiece(index, begin, data));
            Uploaded += data.Length;
        }

        public void SendCancel(int index, int begin, int length) 
        {
            Log.WriteLine(this, "-> cancel");
            SendBytes(EncodeCancel(index, begin, length));
        }

        #endregion

        #region Encoding

        public static byte[] EncodeHandshake(byte[] hash, string id)
        {
            byte[] message = new byte[68];
            message[0] = 19;
            Buffer.BlockCopy(Encoding.UTF8.GetBytes("BitTorrent protocol"), 0, message, 1, 19);
            Buffer.BlockCopy(hash,0, message, 28, 20);
            Buffer.BlockCopy(Encoding.UTF8.GetBytes(id), 0, message, 48, 20);

            return message;
        }

        public static byte[] EncodeKeepAlive()
        {
            return EndianBitConverter.Big.GetBytes(0);
        }

        public static byte[] EncodeChoke() 
        {
            return EncodeState(MessageType.Choke);
        }

        public static byte[] EncodeUnchoke() 
        {
            return EncodeState(MessageType.Unchoke);
        }

        public static byte[] EncodeInterested()
        {
            return EncodeState(MessageType.Interested);
        }

        public static byte[] EncodeNotInterested() 
        {
            return EncodeState(MessageType.NotInterested);
        }

        public static byte[] EncodeState(MessageType type) 
        {
            byte[] message = new byte[5];
            Buffer.BlockCopy(EndianBitConverter.Big.GetBytes(1), 0, message, 0, 4);
            message[4] = (byte)type;
            return message;
        }

        public static byte[] EncodeHave(int index) 
        {            
            byte[] message = new byte[9];
            Buffer.BlockCopy(EndianBitConverter.Big.GetBytes(5), 0, message, 0, 4);
            message[4] = (byte)MessageType.Have;
            Buffer.BlockCopy(EndianBitConverter.Big.GetBytes(index), 0, message, 5, 4);

            return message;
        }

        public static byte[] EncodeBitfield(bool[] isPieceDownloaded) 
        {
            int numPieces = isPieceDownloaded.Length;
            int numBytes = Convert.ToInt32(Math.Ceiling(numPieces / 8.0));
            int numBits = numBytes * 8;

            int length = numBytes + 1;

            byte[] message = new byte[length + 4];
            Buffer.BlockCopy(EndianBitConverter.Big.GetBytes(length), 0, message, 0, 4);
            message[4] = (byte)MessageType.Bitfield;

            bool[] downloaded = new bool[numBits];
            for (int i = 0; i < numPieces; i++)
                downloaded[i] = isPieceDownloaded[i];

            BitArray bitfield = new BitArray(downloaded);
            BitArray reversed = new BitArray(numBits);
            for (int i = 0; i < numBits; i++)
                reversed[i] = bitfield[numBits - i - 1];

            reversed.CopyTo(message, 5);

            return message;
        }

        public static byte[] EncodeRequest(int index, int begin, int length) 
        {
            byte[] message = new byte[17];
            Buffer.BlockCopy(EndianBitConverter.Big.GetBytes(13), 0, message, 0, 4);
            message[4] = (byte)MessageType.Request;
            Buffer.BlockCopy(EndianBitConverter.Big.GetBytes(index), 0, message, 5, 4);
            Buffer.BlockCopy(EndianBitConverter.Big.GetBytes(begin), 0, message, 9, 4);
            Buffer.BlockCopy(EndianBitConverter.Big.GetBytes(length), 0, message, 13, 4);

            return message;
        }

        public static byte[] EncodePiece(int index, int begin, byte[] data) 
        {
            int length = data.Length + 9;

            byte[] message = new byte[length + 4];
            Buffer.BlockCopy(EndianBitConverter.Big.GetBytes(length), 0, message, 0, 4);
            message[4] = (byte)MessageType.Piece;
            Buffer.BlockCopy(EndianBitConverter.Big.GetBytes(index), 0, message, 5, 4);
            Buffer.BlockCopy(EndianBitConverter.Big.GetBytes(begin), 0, message, 9, 4);
            Buffer.BlockCopy(data, 0, message, 13, data.Length);

            return message;
        }

        public static byte[] EncodeCancel(int index, int begin, int length) 
        {
            byte[] message = new byte[17];
            Buffer.BlockCopy(EndianBitConverter.Big.GetBytes(13), 0, message, 0, 4);
            message[4] = (byte)MessageType.Cancel;
            Buffer.BlockCopy(EndianBitConverter.Big.GetBytes(index), 0, message, 5, 4);
            Buffer.BlockCopy(EndianBitConverter.Big.GetBytes(begin), 0, message, 9, 4);
            Buffer.BlockCopy(EndianBitConverter.Big.GetBytes(length), 0, message, 13, 4);

            return message;
        }

        #endregion

        #region Decoding

        public static bool DecodeHandshake(byte[] bytes, out byte[] hash, out string id)
        {
            hash = new byte[20];
            id = "";

            if (bytes.Length != 68 || bytes[0] != 19)
            {
                Log.WriteLine("invalid handshake, must be of length 68 and first byte must equal 19");
                return false;
            }

            if (Encoding.UTF8.GetString(bytes.Skip(1).Take(19).ToArray()) != "BitTorrent protocol")
            {
                Log.WriteLine("invalid handshake, protocol must equal \"BitTorrent protocol\"");
                return false;
            }

            // flags
            //byte[] flags = bytes.Skip(20).Take(8).ToArray();

            hash = bytes.Skip(28).Take(20).ToArray();

            id = Encoding.UTF8.GetString(bytes.Skip(48).Take(20).ToArray());

            return true;
        }

        public static bool DecodeKeepAlive(byte[] bytes)
        {            
            if (bytes.Length != 4 || EndianBitConverter.Big.ToInt32(bytes,0) != 0 )
            {
                Log.WriteLine("invalid keep alive");
                return false;
            }
            return true;
        }

        public static bool DecodeChoke(byte[] bytes)
        {            
            return DecodeState(bytes, MessageType.Choke);
        }

        public static bool DecodeUnchoke(byte[] bytes)
        {            
            return DecodeState(bytes, MessageType.Unchoke);
        }

        public static bool DecodeInterested(byte[] bytes)
        {            
            return DecodeState(bytes, MessageType.Interested);
        }

        public static bool DecodeNotInterested(byte[] bytes)
        {            
            return DecodeState(bytes, MessageType.NotInterested);
        }

        public static bool DecodeState(byte[] bytes, MessageType type)
        {            
            if (bytes.Length != 5 || EndianBitConverter.Big.ToInt32(bytes, 0) != 1 || bytes[4] != (byte)type)
            {
                Log.WriteLine("invalid " + Enum.GetName(typeof(MessageType), type));
                return false;
            }
            return true;
        }

        public static bool DecodeHave(byte[] bytes, out int index)
        {
            index = -1;

            if (bytes.Length != 9 || EndianBitConverter.Big.ToInt32(bytes, 0) != 5)
            {
                Log.WriteLine("invalid have, first byte must equal 0x2");
                return false;
            }

            index = EndianBitConverter.Big.ToInt32(bytes, 5);

            return true;
        }

        public static bool DecodeBitfield(byte[] bytes, int pieces, out bool[] isPieceDownloaded)
        {
            isPieceDownloaded = new bool[pieces];

            int expectedLength = Convert.ToInt32(Math.Ceiling(pieces / 8.0)) + 1;

            if (bytes.Length != expectedLength + 4 || EndianBitConverter.Big.ToInt32(bytes,0) != expectedLength)
            {
                Log.WriteLine("invalid bitfield, first byte must equal " + expectedLength);
                return false;
            }

            BitArray bitfield = new BitArray(bytes.Skip(5).ToArray());

            for (int i = 0; i < pieces; i++)
                isPieceDownloaded[i] = bitfield[bitfield.Length - 1 - i];

            return true;
        }

        public static bool DecodeRequest(byte[] bytes, out int index, out int begin, out int length)
        {
            index = -1;
            begin = -1;
            length = -1;

            if (bytes.Length != 17 || EndianBitConverter.Big.ToInt32(bytes,0) != 13)
            {
                Log.WriteLine("invalid request message, must be of length 17");
                return false;
            }

            index = EndianBitConverter.Big.ToInt32(bytes, 5);
            begin = EndianBitConverter.Big.ToInt32(bytes, 9);
            length = EndianBitConverter.Big.ToInt32(bytes, 13);

            return true;
        }

        public static bool DecodePiece(byte[] bytes, out int index, out int begin, out byte[] data)
        {
            index = -1;
            begin = -1;
            data = new byte[0];

            if (bytes.Length < 13)
            {
                Log.WriteLine("invalid piece message");
                return false;
            }

            int length = EndianBitConverter.Big.ToInt32(bytes, 0) - 9;
            index = EndianBitConverter.Big.ToInt32(bytes, 5);
            begin = EndianBitConverter.Big.ToInt32(bytes, 9);

            data = new byte[length];
            Buffer.BlockCopy(bytes, 13, data, 0, length);

            return true;
        }

        public static bool DecodeCancel(byte[] bytes, out int index, out int begin, out int length)
        {
            index = -1;
            begin = -1;
            length = -1;

            if (bytes.Length != 17 || EndianBitConverter.Big.ToInt32(bytes,0) != 13)
            {
                Log.WriteLine("invalid cancel message, must be of length 17");
                return false;
            }

            index = EndianBitConverter.Big.ToInt32(bytes, 5);
            begin = EndianBitConverter.Big.ToInt32(bytes, 9);
            length = EndianBitConverter.Big.ToInt32(bytes, 13);

            return true;
        }

        #endregion

        #region Helper

        public override string ToString()
        {
            return string.Format("[{0} ({1})]", IPEndPoint, Id);
        }

        #endregion
    }
}