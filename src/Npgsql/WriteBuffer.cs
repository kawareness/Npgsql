#region License
// The PostgreSQL License
//
// Copyright (C) 2015 The Npgsql Development Team
//
// Permission to use, copy, modify, and distribute this software and its
// documentation for any purpose, without fee, and without a written
// agreement is hereby granted, provided that the above copyright notice
// and this paragraph and the following two paragraphs appear in all copies.
//
// IN NO EVENT SHALL THE NPGSQL DEVELOPMENT TEAM BE LIABLE TO ANY PARTY
// FOR DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES,
// INCLUDING LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
// DOCUMENTATION, EVEN IF THE NPGSQL DEVELOPMENT TEAM HAS BEEN ADVISED OF
// THE POSSIBILITY OF SUCH DAMAGE.
//
// THE NPGSQL DEVELOPMENT TEAM SPECIFICALLY DISCLAIMS ANY WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
// AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS
// ON AN "AS IS" BASIS, AND THE NPGSQL DEVELOPMENT TEAM HAS NO OBLIGATIONS
// TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
#endregion

using System;
using System.Diagnostics.Contracts;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Text;
using AsyncRewriter;

namespace Npgsql
{
    internal partial class WriteBuffer
    {
        #region Fields and Properties

        internal Socket Socket { get; }

        /// <summary>
        /// The total byte length of the buffer.
        /// </summary>
        internal int Size { get; }

        /// <summary>
        /// During copy operations, the buffer's usable size is smaller than its total size because of the CopyData
        /// message header. This distinction is important since some type handlers check how much space is left
        /// in the buffer in their decision making.
        /// </summary>
        internal int UsableSize
        {
            get { return _usableSize; }
            set
            {
                Contract.Requires(value <= Size);
                _usableSize = value;
            }
        }

        int _usableSize;
        internal Encoding TextEncoding { get; }

        internal int End { get { return _end; } set { _end = value; } }
        internal int WriteSpaceLeft => Size - _end;

        internal long TotalBytesWritten { get; private set; }

        internal byte[] Data { get; }
        readonly Encoder _textEncoder;

        int _end;

        /// <summary>
        /// Used for internal temporary purposes
        /// </summary>
        readonly char[] _tempCharBuf;

        BitConverterUnion _bitConverterUnion;

        /// <summary>
        /// The minimum buffer size possible.
        /// </summary>
        internal const int MinimumSize = 4096;
        internal const int DefaultBufferSize = 8192;

        #endregion

        #region Constructors

        internal WriteBuffer(Socket socket, int size, Encoding textEncoding)
            : this(size, textEncoding)
        {
            Socket = socket;
        }

        internal WriteBuffer(int size, Encoding textEncoding)
        {
            if (size < MinimumSize)
            {
                throw new ArgumentOutOfRangeException(nameof(size), size, "Buffer size must be at least " + MinimumSize);
            }
            Contract.EndContractBlock();

            Size = size;
            UsableSize = Size;
            Data = new byte[Size];
            TextEncoding = textEncoding;
            _textEncoder = TextEncoding.GetEncoder();
            _tempCharBuf = new char[1024];
        }

        #endregion

        #region I/O

        [RewriteAsync]
        internal void Send()
        {
            if (_end != 0)
            {
                var count = Socket.Send(Data, 0, _end, SocketFlags.None);
                TotalBytesWritten += count;
                if (count == _end)
                {
                    _end = 0;
                }
            }
        }

        #endregion

        #region Write Simple

        public void WriteByte(byte b)
        {
            Contract.Requires(WriteSpaceLeft >= sizeof(byte));
            Data[_end++] = b;
        }

        public void WriteInt16(int i)
        {
            Contract.Requires(WriteSpaceLeft >= sizeof(short));
            Data[_end++] = (byte)(i >> 8);
            Data[_end++] = (byte)i;
        }

        public void WriteInt32(int i)
        {
            Contract.Requires(WriteSpaceLeft >= sizeof(int));
            var pos = _end;
            Data[pos++] = (byte)(i >> 24);
            Data[pos++] = (byte)(i >> 16);
            Data[pos++] = (byte)(i >> 8);
            Data[pos++] = (byte)i;
            _end = pos;
        }

        internal void WriteUInt32(uint i)
        {
            Contract.Requires(WriteSpaceLeft >= sizeof(uint));
            var pos = _end;
            Data[pos++] = (byte)(i >> 24);
            Data[pos++] = (byte)(i >> 16);
            Data[pos++] = (byte)(i >> 8);
            Data[pos++] = (byte)i;
            _end = pos;
        }

        public void WriteInt64(long i)
        {
            Contract.Requires(WriteSpaceLeft >= sizeof(long));
            var pos = _end;
            Data[pos++] = (byte)(i >> 56);
            Data[pos++] = (byte)(i >> 48);
            Data[pos++] = (byte)(i >> 40);
            Data[pos++] = (byte)(i >> 32);
            Data[pos++] = (byte)(i >> 24);
            Data[pos++] = (byte)(i >> 16);
            Data[pos++] = (byte)(i >> 8);
            Data[pos++] = (byte)i;
            _end = pos;
        }

        public void WriteSingle(float f)
        {
            Contract.Requires(WriteSpaceLeft >= sizeof(float));
            _bitConverterUnion.float4 = f;
            var pos = _end;
            if (BitConverter.IsLittleEndian)
            {
                Data[pos++] = _bitConverterUnion.b3;
                Data[pos++] = _bitConverterUnion.b2;
                Data[pos++] = _bitConverterUnion.b1;
                Data[pos++] = _bitConverterUnion.b0;
            }
            else
            {
                Data[pos++] = _bitConverterUnion.b0;
                Data[pos++] = _bitConverterUnion.b1;
                Data[pos++] = _bitConverterUnion.b2;
                Data[pos++] = _bitConverterUnion.b3;
            }
            _end = pos;
        }

        public void WriteDouble(double d)
        {
            Contract.Requires(WriteSpaceLeft >= sizeof(double));
            _bitConverterUnion.float8 = d;
            var pos = _end;
            if (BitConverter.IsLittleEndian)
            {
                Data[pos++] = _bitConverterUnion.b7;
                Data[pos++] = _bitConverterUnion.b6;
                Data[pos++] = _bitConverterUnion.b5;
                Data[pos++] = _bitConverterUnion.b4;
                Data[pos++] = _bitConverterUnion.b3;
                Data[pos++] = _bitConverterUnion.b2;
                Data[pos++] = _bitConverterUnion.b1;
                Data[pos++] = _bitConverterUnion.b0;
            }
            else
            {
                Data[pos++] = _bitConverterUnion.b0;
                Data[pos++] = _bitConverterUnion.b1;
                Data[pos++] = _bitConverterUnion.b2;
                Data[pos++] = _bitConverterUnion.b3;
                Data[pos++] = _bitConverterUnion.b4;
                Data[pos++] = _bitConverterUnion.b5;
                Data[pos++] = _bitConverterUnion.b6;
                Data[pos++] = _bitConverterUnion.b7;
            }
            _end = pos;
        }

        internal void WriteString(string s, int len = 0)
        {
            Contract.Requires(TextEncoding.GetByteCount(s) <= WriteSpaceLeft);
            End += TextEncoding.GetBytes(s, 0, len == 0 ? s.Length : len, Data, End);
        }

        internal void WriteChars(char[] chars, int len = 0)
        {
            Contract.Requires(TextEncoding.GetByteCount(chars) <= WriteSpaceLeft);
            End += TextEncoding.GetBytes(chars, 0, len == 0 ? chars.Length : len, Data, End);
        }

        public void WriteBytes(byte[] buf, int offset, int count)
        {
            Contract.Requires(count <= WriteSpaceLeft);
            Buffer.BlockCopy(buf, offset, Data, End, count);
            End += count;
        }

        public void WriteBytesNullTerminated(byte[] buf)
        {
            Contract.Requires(WriteSpaceLeft >= buf.Length + 1);
            WriteBytes(buf, 0, buf.Length);
            WriteByte(0);
        }

        #endregion

        #region Write Complex

        internal void WriteStringChunked(char[] chars, int charIndex, int charCount,
                                         bool flush, out int charsUsed, out bool completed)
        {
            int bytesUsed;
            _textEncoder.Convert(chars, charIndex, charCount, Data, End, WriteSpaceLeft,
                                 flush, out charsUsed, out bytesUsed, out completed);
            End += bytesUsed;
        }

        #endregion

        #region Misc

        internal void Clear()
        {
            End = 0;
        }

        internal void ResetTotalBytesWritten()
        {
            TotalBytesWritten = 0;
        }

        [StructLayout(LayoutKind.Explicit, Size = 8)]
        struct BitConverterUnion
        {
            [FieldOffset(0)] public readonly byte b0;
            [FieldOffset(1)] public readonly byte b1;
            [FieldOffset(2)] public readonly byte b2;
            [FieldOffset(3)] public readonly byte b3;
            [FieldOffset(4)] public readonly byte b4;
            [FieldOffset(5)] public readonly byte b5;
            [FieldOffset(6)] public readonly byte b6;
            [FieldOffset(7)] public readonly byte b7;

            [FieldOffset(0)] public float float4;
            [FieldOffset(0)] public double float8;
        }

        #endregion
    }
}
