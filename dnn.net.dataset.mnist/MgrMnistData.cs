using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using MyCaffe.imagedb;
using MyCaffe.common;
using MyCaffe.basecode;
using MyCaffe.data;
using System.IO.Compression;

namespace DNN.net.dataset.mnist
{
    public class MgrMnistData : IDisposable 
    {
        DatasetFactory m_factory;
        Log m_log;

        public event EventHandler<LoadStartArgs> OnLoadStart;
        public event EventHandler<LoadArgs> OnLoadProgress;
        public event EventHandler<LoadErrorArgs> OnLoadError;

        public MgrMnistData(DatasetFactory factory, Log log, MyCaffeImageDatabase imgDb = null)
        {
            m_factory = factory;
            m_log = log;
        }

        public void Dispose()
        {
        }

        private string expandFile(string strFile)
        {
            FileInfo fi = new FileInfo(strFile);
            string strNewFile = fi.DirectoryName;
            int nPos = fi.Name.LastIndexOf('.');

            if (nPos >= 0)
                strNewFile += "\\" + fi.Name.Substring(0, nPos) + ".bin";
            else
                strNewFile += "\\" + fi.Name + ".bin";

            if (!File.Exists(strNewFile))
            {
                using (FileStream fs = fi.OpenRead())
                {
                    using (FileStream fsBin = File.Create(strNewFile))
                    {
                        using (GZipStream decompStrm = new GZipStream(fs, CompressionMode.Decompress))
                        {
                            decompStrm.CopyTo(fsBin);
                        }
                    }
                }
            }

            return strNewFile;
        }

        public uint ConvertData(string strImageFile, string strLabelFile, string strDBPath, bool bGetItemCountOnly = false, int nChannels = 1)
        {
            string strExt;

            strExt = Path.GetExtension(strImageFile).ToLower();
            if (strExt == ".gz")
            {
                m_log.WriteLine("Unpacking '" + strImageFile + "'...");
                strImageFile = expandFile(strImageFile);
            }

            strExt = Path.GetExtension(strLabelFile).ToLower();
            if (strExt == ".gz")
            {
                m_log.WriteLine("Unpacking '" + strLabelFile + "'...");
                strLabelFile = expandFile(strLabelFile);
            }

            BinaryFile image_file = new BinaryFile(strImageFile);
            BinaryFile label_file = new BinaryFile(strLabelFile);

            try
            {
                uint magicImg = image_file.ReadUInt32();
                uint magicLbl = label_file.ReadUInt32();

                if (magicImg != 2051)
                {
                    if (m_log != null)
                        m_log.FAIL("Incorrect image file magic.");

                    if (OnLoadError != null)
                        OnLoadError(this, new LoadErrorArgs("Incorrect image file magic."));
                }

                if (magicLbl != 2049)
                {
                    if (m_log != null)
                        m_log.FAIL("Incorrect label file magic.");

                    if (OnLoadError != null)
                        OnLoadError(this, new LoadErrorArgs("Incorrect label file magic."));
                }

                uint num_items = image_file.ReadUInt32();
                uint num_labels = label_file.ReadUInt32();

                if (num_items != num_labels)
                {
                    if (m_log != null)
                        m_log.FAIL("The number of items must equal the number of labels.");

                    throw new Exception("The number of items must equal the number of labels." + Environment.NewLine + "  Label File: '" + strLabelFile + Environment.NewLine + "  Image File: '" + strImageFile + "'.");
                }

                if (bGetItemCountOnly)
                    return num_items;

                uint rows = image_file.ReadUInt32();
                uint cols = image_file.ReadUInt32();

                int nSrcId = m_factory.AddSource(strDBPath, nChannels, (int)cols, (int)rows, false, 0, true);
                m_factory.Open(nSrcId);
                m_factory.DeleteSourceData();

                // Storing to db
                byte[] rgLabel;
                byte[] rgPixels;

                Datum datum = new Datum(false, nChannels, (int)cols, (int)rows, -1, DateTime.MinValue, null, null, 0, false, -1);

                if (m_log != null)
                {
                    m_log.WriteHeader("LOADING " + strDBPath + " items.");
                    m_log.WriteLine("A total of " + num_items.ToString() + " items.");
                    m_log.WriteLine("Rows: " + rows.ToString() + " Cols: " + cols.ToString());
                }

                if (OnLoadStart != null)
                    OnLoadStart(this, new LoadStartArgs((int)num_items));

                for (int item_id = 0; item_id < num_items; item_id++)
                {
                    rgPixels = image_file.ReadBytes((int)(rows * cols));
                    rgLabel = label_file.ReadBytes(1);

                    List<byte> rgData = new List<byte>(rgPixels);

                    if (nChannels == 3)
                    {
                        rgData.AddRange(new List<byte>(rgPixels));
                        rgData.AddRange(new List<byte>(rgPixels));
                    }

                    datum.SetData(rgData, (int)rgLabel[0]);
                    m_factory.PutRawImageCache(item_id, datum);

                    if ((item_id % 1000) == 0)
                    {
                        if (m_log != null)
                        {
                            m_log.WriteLine("Loaded " + item_id.ToString("N") + " items...");
                            m_log.Progress = (double)item_id / (double)num_items;
                        }

                        if (OnLoadProgress != null)
                        {
                            LoadArgs args = new LoadArgs(item_id);
                            OnLoadProgress(this, args);

                            if (args.Cancel)
                                break;
                        }
                    }
                }

                m_factory.ClearImageCash(true);
                m_factory.UpdateSourceCounts();

                if (OnLoadProgress != null)
                {
                    LoadArgs args = new LoadArgs((int)num_items);
                    OnLoadProgress(this, args);
                }

                return num_items;
            }
            finally
            {
                image_file.Dispose();
                label_file.Dispose();
            }
        }


        public void DeleteData(int nSrcID)
        {
            m_factory.DeleteSourceData(nSrcID);
        }
    }

    public class LoadStartArgs : EventArgs
    {
        int m_nCount;

        public LoadStartArgs(int nCount)
        {
            m_nCount = nCount;
        }

        public int Count
        {
            get { return m_nCount; }
        }
    }

    public class LoadArgs : EventArgs
    {
        int m_nItem;
        bool m_bCancel = false;

        public LoadArgs(int nItem)
        {
            m_nItem = nItem;
        }

        public int Item
        {
            get { return m_nItem; }
        }

        public bool Cancel
        {
            get { return m_bCancel; }
            set { m_bCancel = value; }
        }
    }

    public class LoadErrorArgs : EventArgs
    {
        string m_strErr;

        public LoadErrorArgs(string str)
        {
            m_strErr = str;
        }

        public string Error
        {
            get { return m_strErr; }
        }
    }

    class BinaryFile : IDisposable 
    {
        FileStream m_file;
        BinaryReader m_reader;

        public BinaryFile(string strFile)
        {
            m_file = File.Open(strFile, FileMode.Open, FileAccess.Read, FileShare.Read);
            m_reader = new BinaryReader(m_file);
        }

        public void Dispose()
        {
            m_reader.Close();
        }

        public BinaryReader Reader
        {
            get { return m_reader; }
        }

        public UInt32 ReadUInt32()
        {
            UInt32 nVal = m_reader.ReadUInt32();

            return swap_endian(nVal);
        }

        public byte[] ReadBytes(int nCount)
        {
            return m_reader.ReadBytes(nCount);
        }

        private UInt32 swap_endian(UInt32 nVal)
        {
            nVal = ((nVal << 8) & 0xFF00FF00) | ((nVal >> 8) & 0x00FF00FF);
            return (nVal << 16) | (nVal >> 16);
        }
    }
}
