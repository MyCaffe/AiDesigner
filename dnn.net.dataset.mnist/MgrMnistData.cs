using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using MyCaffe.db.image;
using MyCaffe.common;
using MyCaffe.basecode;
using MyCaffe.data;
using System.IO.Compression;
using System.Threading;
using System.Drawing;

namespace DNN.net.dataset.mnist
{
    public class MgrMnistData : IDisposable 
    {
        DatasetFactory m_factory;
        Log m_log;
        Bitmap m_bmpTargetOverlay = null;
        Random m_random = null;

        public event EventHandler<LoadStartArgs> OnLoadStart;
        public event EventHandler<LoadArgs> OnLoadProgress;
        public event EventHandler<LoadErrorArgs> OnLoadError;

        public MgrMnistData(DatasetFactory factory, Log log, string strTargetFileOverlay, MyCaffeImageDatabase imgDb = null)
        {
            m_factory = factory;
            m_log = log;

            if (!string.IsNullOrEmpty(strTargetFileOverlay) && File.Exists(strTargetFileOverlay))
            {
                m_bmpTargetOverlay = new Bitmap(strTargetFileOverlay);
                m_random = new Random();
            }
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

        private Datum createTargetOverlay(Datum datum)
        {
            int nOffsetMaxX = m_bmpTargetOverlay.Width / datum.Width;
            int nOffsetMaxY = m_bmpTargetOverlay.Height / datum.Height;
            int nOffsetX = m_random.Next(nOffsetMaxX) * datum.Width;
            int nOffsetY = m_random.Next(nOffsetMaxY) * datum.Height;

            Bitmap bmpDst = new Bitmap(datum.Width, datum.Height);
            LockBitmap bmpD = new LockBitmap(bmpDst);
            LockBitmap bmpO = new LockBitmap(m_bmpTargetOverlay);

            bmpD.LockBits();
            bmpO.LockBits();

            for (int y = 0; y < bmpDst.Width; y++)
            {
                for (int x = 0; x < bmpDst.Height; x++)
                {
                    int nIdx = y * datum.Width + x;
                    byte bClr = datum.ByteData[nIdx];
                    Color clrOvl = bmpO.GetPixel(nOffsetX + x, nOffsetY + y);

                    // Invert the color where the data is in the original
                    // MNIST datum.
                    if (bClr != 0)
                    {
                        int nR = 255 - clrOvl.R;
                        int nG = 255 - clrOvl.G;
                        int nB = 255 - clrOvl.R;

                        clrOvl = Color.FromArgb(nR, nG, nB);
                    }

                    bmpD.SetPixel(x, y, clrOvl);
                }
            }

            bmpO.UnlockBits();
            bmpD.UnlockBits();

            return ImageData.GetImageDataD(bmpDst, 3, false, datum.Label);
        }

        public uint ConvertData(string strImageFile, string strLabelFile, string strDBPath, bool bCreateImgMean, bool bGetItemCountOnly = false, int nChannels = 1)
        {
            string strExt;
            List<SimpleDatum> rgImg = new List<SimpleDatum>();

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
                m_factory.Open(nSrcId, 500, true); // use file based data.
                m_factory.DeleteSourceData();

                // Storing to db
                byte[] rgLabel;
                byte[] rgPixels;

                Datum datum = new Datum(false, nChannels, (int)cols, (int)rows, -1, DateTime.MinValue, (List<double>)null, 0, false, -1);

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

                    if (m_bmpTargetOverlay != null)
                        datum = createTargetOverlay(datum);

                    m_factory.PutRawImageCache(item_id, datum);

                    if (bCreateImgMean)
                        rgImg.Add(new SimpleDatum(datum));

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

                m_factory.ClearImageCashe(true);
                m_factory.UpdateSourceCounts();

                if (bCreateImgMean)
                {
                    m_log.WriteLine("Creating image mean...");
                    SimpleDatum dMean = SimpleDatum.CalculateMean(m_log, rgImg.ToArray(), new WaitHandle[] { new ManualResetEvent(false) });
                    m_factory.PutRawImageMean(dMean, true);
                }

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
