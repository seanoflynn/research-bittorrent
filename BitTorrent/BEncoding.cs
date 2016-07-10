using System;
using System.Text;
using System.Linq;
using System.Collections.Generic;
using System.IO;

namespace BitTorrent
{
    public static class BEncoding
    {
        private const byte DictionaryStart  = (byte)'d'; // 100
        private const byte DictionaryEnd    = (byte)'e'; // 101
        private const byte ListStart        = (byte)'l'; // 108
        private const byte ListEnd          = (byte)'e'; // 101
        private const byte NumberStart      = (byte)'i'; // 105
        private const byte NumberEnd        = (byte)'e'; // 101
        private const byte ByteArrayDivider = (byte)':'; //  58

        #region Decode

        public static object Decode(byte[] bytes)
        {
            IEnumerator<byte> enumerator = ((IEnumerable<byte>)bytes).GetEnumerator();
            enumerator.MoveNext();

            return DecodeNextObject(enumerator);
        }

        public static object DecodeFile(string path)
        {
            byte[] bytes = File.ReadAllBytes(path);

            return BEncoding.Decode(bytes);
        }

        private static object DecodeNextObject(IEnumerator<byte> enumerator)
        {
            if (enumerator.Current == DictionaryStart)
                return DecodeDictionary(enumerator);            

            if (enumerator.Current == ListStart)
                return DecodeList(enumerator);            

            if (enumerator.Current == NumberStart)                       
                return DecodeNumber(enumerator);

            return DecodeByteArray(enumerator);
        }
            
        private static Dictionary<string,object> DecodeDictionary(IEnumerator<byte> enumerator)
        {            
            Dictionary<string,object> dict = new Dictionary<string,object>();
            List<string> keys = new List<string>();

            // keep decoding objects until we hit the end flag
            while (enumerator.MoveNext())
            {
                if( enumerator.Current == DictionaryEnd )
                    break;

                // all keys are valid UTF8 strings
                string key = Encoding.UTF8.GetString(DecodeByteArray(enumerator));
                enumerator.MoveNext();
                object val = DecodeNextObject(enumerator);

                keys.Add(key);
                dict.Add(key, val);
            }

            // verify incoming dictionary is sorted correctly
            // we will not be able to create an identical encoding otherwise
            var sortedKeys = keys.OrderBy(x => BitConverter.ToString(Encoding.UTF8.GetBytes(x)));
            if (!keys.SequenceEqual(sortedKeys))
                throw new Exception("error loading dictionary: keys not sorted");

            return dict;
        }

        private static List<object> DecodeList(IEnumerator<byte> enumerator)
        {
            List<object> list = new List<object>();

            // keep decoding objects until we hit the end flag
            while (enumerator.MoveNext())
            {
                if( enumerator.Current == ListEnd )
                    break;

                list.Add(DecodeNextObject(enumerator));
            }

            return list;
        }

        private static byte[] DecodeByteArray(IEnumerator<byte> enumerator)
        {
            List<byte> lengthBytes = new List<byte>();

            // scan until we get to divider
            do
            {
                if( enumerator.Current == ByteArrayDivider )
                    break;

                lengthBytes.Add(enumerator.Current);
            }
            while (enumerator.MoveNext());

            string lengthString = System.Text.Encoding.UTF8.GetString(lengthBytes.ToArray());

            int length;
            if (!Int32.TryParse(lengthString, out length))
                throw new Exception("unable to parse length of byte array");

            // now read in the actual byte array
            byte[] bytes = new byte[length];

            for (int i = 0; i < length; i++)
            {
                enumerator.MoveNext();
                bytes[i] = enumerator.Current;
            }

            return bytes;
        }

        private static long DecodeNumber(IEnumerator<byte> enumerator)
        {
            List<byte> bytes = new List<byte>();

            // keep pulling bytes until we hit the end flag
            while (enumerator.MoveNext())
            {
                if (enumerator.Current == NumberEnd)
                    break;
                
                bytes.Add(enumerator.Current);
            }

            string numAsString = Encoding.UTF8.GetString(bytes.ToArray());

            return Int64.Parse(numAsString);
        }

        #endregion

        #region Encode

        public static byte[] Encode(object obj)
        {
            MemoryStream buffer = new MemoryStream();

            EncodeNextObject(buffer, obj);

            return buffer.ToArray();
        }

        public static void EncodeToFile(object obj, string path)
        {
            File.WriteAllBytes(path, Encode(obj));
        }

        private static void EncodeNextObject(MemoryStream buffer, object obj)
        {
            if (obj is byte[])                
                EncodeByteArray(buffer, (byte[])obj);
            else if (obj is string)
                EncodeString(buffer, (string)obj);
            else if (obj is long)
                EncodeNumber(buffer, (long)obj);
            else if (obj.GetType() == typeof(List<object>))
                EncodeList(buffer, (List<object>)obj);
            else if (obj.GetType() == typeof(Dictionary<string,object>))
                EncodeDictionary(buffer, (Dictionary<string,object>)obj);
            else
                throw new Exception("unable to encode type " + obj.GetType());
        }

        private static void EncodeByteArray(MemoryStream buffer, byte[] body)
        {                        
            buffer.Append(Encoding.UTF8.GetBytes(Convert.ToString(body.Length)));
            buffer.Append(ByteArrayDivider);
            buffer.Append(body);
        }

        private static void EncodeString(MemoryStream buffer, string input)
        {   
            EncodeByteArray(buffer, Encoding.UTF8.GetBytes(input));
        }

        private static void EncodeNumber(MemoryStream buffer, long input)
        {
            buffer.Append(NumberStart);
            buffer.Append(Encoding.UTF8.GetBytes(Convert.ToString(input)));
            buffer.Append(NumberEnd);
        }

        private static void EncodeList(MemoryStream buffer, List<object> input)
        {
            buffer.Append(ListStart);
            foreach (var item in input)
                EncodeNextObject(buffer, item);
            buffer.Append(ListEnd);
        }

        private static void EncodeDictionary(MemoryStream buffer, Dictionary<string,object> input)
        {
            buffer.Append(DictionaryStart);

            // we need to sort the keys by their raw bytes, not the string
            var sortedKeys = input.Keys.ToList().OrderBy(x => BitConverter.ToString(Encoding.UTF8.GetBytes(x)));

            foreach (var key in sortedKeys)
            {
                EncodeString(buffer, key);
                EncodeNextObject(buffer, input[key]);
            }
            buffer.Append(DictionaryEnd);
        }

        #endregion

        #region Helper

        public static string GetFormattedString(object obj, int depth = 0)
        {
            string output = "";
            
            if (obj is byte[])
                output += GetFormattedString((byte[])obj);
            else if (obj is long)
                output += GetFormattedString((long)obj);
            else if (obj.GetType() == typeof(List<object>))
                output += GetFormattedString((List<object>)obj, depth);
            else if (obj.GetType() == typeof(Dictionary<string,object>))
                output += GetFormattedString((Dictionary<string,object>)obj, depth);
            else
                throw new Exception("unable to encode type " + obj.GetType());

            return output;
        }

        private static string GetFormattedString(byte[] obj)
        {
            return String.Join("", obj.Select(x => x.ToString("x2"))) + " (" + Encoding.UTF8.GetString(obj) + ")";
        }

        private static string GetFormattedString(long obj)
        {
            return obj.ToString();
        }

        private static string GetFormattedString(List<object> obj, int depth)
        {
            string pad1 = new String(' ', depth * 2);
            string pad2 = new String(' ', (depth+1) * 2);

            if (obj.Count < 1)
                return "[]";

            if (obj[0].GetType() == typeof(Dictionary<string,object>))
                return "\n" + pad1 + "[" + String.Join(",", obj.Select(x => pad2 + GetFormattedString(x, depth + 1))) + "\n" + pad1 + "]";
            
            return "[ " + String.Join(", ", obj.Select(x => GetFormattedString(x))) + " ]";
        }

        private static string GetFormattedString(Dictionary<string,object> obj, int depth)
        {
            string pad1 = new String(' ', depth * 2);
            string pad2 = new String(' ', (depth+1) * 2);

            return (depth>0?"\n":"") + pad1 + "{" + String.Join("", obj.Select(x => "\n" + pad2 + (x.Key+":").PadRight(15,' ') + GetFormattedString(x.Value, depth+1))) + "\n" + pad1 + "}";
            //return String.Join("", obj.Select(x => "\n" + pad2 + (x.Key+":").PadRight(15,' ') + GetFormattedString(x.Value, depth+1)));
        }

        #endregion
    }

    // source: Fredrik Mörk (http://stackoverflow.com/a/4015634)
    public static class MemoryStreamExtensions
    {
        public static void Append(this MemoryStream stream, byte value)
        {
            stream.WriteByte(value);
        }

        public static void Append(this MemoryStream stream, byte[] values)
        {
            stream.Write(values, 0, values.Length);
        }
    }
}

