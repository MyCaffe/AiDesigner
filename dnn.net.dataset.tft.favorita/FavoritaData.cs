using MyCaffe.basecode;
using MyCaffe.common;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace DNN.net.dataset.tft.favorita
{
    public class FavoritaData
    {
        Dictionary<int, StoreRecord> m_rgStores = new Dictionary<int, StoreRecord>();
        Dictionary<int, ItemRecord> m_rgItems = new Dictionary<int, ItemRecord>();
        Dictionary<DateTime, HolidayRecord> m_rgHolidaysLocal = new Dictionary<DateTime, HolidayRecord>();
        Dictionary<DateTime, HolidayRecord> m_rgHolidaysRegional = new Dictionary<DateTime, HolidayRecord>();
        Dictionary<DateTime, HolidayRecord> m_rgHolidaysNational = new Dictionary<DateTime, HolidayRecord>();
        Dictionary<DateTime, float> m_rgOil = new Dictionary<DateTime, float>();
        Dictionary<int, Dictionary<DateTime, TransactionRecord>> m_rgTrx = new Dictionary<int, Dictionary<DateTime, TransactionRecord>>();
        DataRecordCollection m_rgRecords;
        Dictionary<string, string> m_rgFiles;
        Log m_log;
        CancelEvent m_evtCancel;


        public FavoritaData(Dictionary<string, string> rgFiles, Log log, CancelEvent evtCancel)
        {
            m_rgFiles = rgFiles;
            m_log = log;
            m_evtCancel = evtCancel;
            m_rgRecords = new DataRecordCollection(log, evtCancel);
        }

        public bool LoadData(DateTime dtStart, DateTime dtEnd)
        {
            m_rgRecords.Clear();
            m_rgStores.Clear();
            m_rgItems.Clear();
            m_rgHolidaysNational.Clear();
            m_rgHolidaysRegional.Clear();
            m_rgHolidaysLocal.Clear();
            m_rgOil.Clear();

            if (!loadTemporalData("Train", dtStart, dtEnd))
                return false;

            if (!loadStoreData())
                return false;

            if (!loadItemData())
                return false;

            if (!loadHolidayData(dtStart, dtEnd))
                return false;

            if (!loadTransactionData(dtStart, dtEnd))
                return false;

            if (!loadOilData(dtStart, dtEnd))
                return false;

            if (!m_rgRecords.AddStoreInfo(m_rgStores))
                return false;

            if (!m_rgRecords.AddItemInfo(m_rgItems))
                return false;

            if (!m_rgRecords.AddTransactionInfo(m_rgTrx))
                return false;

            if (!m_rgRecords.AddHolidayInfo(m_rgHolidaysNational))
                return false;

            if (!m_rgRecords.AddHolidayInfo(m_rgHolidaysRegional))
                return false;

            if (!m_rgRecords.AddHolidayInfo(m_rgHolidaysLocal))
                return false;

            if (!m_rgRecords.AddOil(m_rgOil))
                return false;

            return true;
        }

        private bool loadTemporalData(string strType, DateTime dtStart, DateTime dtEnd)
        {
            if (strType != "Train" && strType != "Test")
                throw new Exception("Unknown temporal type '" + strType + "' specified.");

            string strFile = m_rgFiles[strType];
            string strLine = null;

            FileInfo fi = new FileInfo(strFile);
            long lRead = 0;
            Stopwatch sw = new Stopwatch();

            sw.Start();

            using (StreamReader sr = new StreamReader(strFile))
            {
                strLine = sr.ReadLine();
                strLine = sr.ReadLine();

                while (!string.IsNullOrEmpty(strLine))
                {
                    lRead += strLine.Length + 2;
                    string[] rgstr = strLine.Split(',');

                    DataRecord rec = DataRecord.Parse(rgstr, dtStart, dtEnd);
                    if (rec != null)
                        m_rgRecords.Add(rec);

                    strLine = sr.ReadLine();

                    if (sw.Elapsed.TotalMilliseconds > 1000)
                    {
                        double dfPct = (double)lRead / (double)fi.Length;
                        m_log.WriteLine("Loading " + strType + " data, " + dfPct.ToString("P4") + " complete.", true);
                        sw.Restart();

                        if (m_evtCancel.WaitOne(0))
                            return false;
                    }
                }
            }

            return true;
        }

        private bool loadStoreData()
        {
            string strType = "Stores";
            string strFile = m_rgFiles[strType];
            string strLine = null;

            FileInfo fi = new FileInfo(strFile);
            long lRead = 0;
            Stopwatch sw = new Stopwatch();

            sw.Start();

            using (StreamReader sr = new StreamReader(strFile))
            {
                strLine = sr.ReadLine();
                strLine = sr.ReadLine();

                while (!string.IsNullOrEmpty(strLine))
                {
                    lRead += strLine.Length + 2;

                    string[] rgstr = strLine.Split(',');
                    StoreRecord rec = new StoreRecord(rgstr);
                    m_rgStores.Add(rec.StoreNum, rec);
                    strLine = sr.ReadLine();

                    if (sw.Elapsed.TotalMilliseconds > 1000)
                    {
                        double dfPct = (double)lRead / (double)fi.Length;
                        m_log.WriteLine("Loading " + strType + " data, " + dfPct.ToString("P4") + " complete.", true);
                        sw.Restart();

                        if (m_evtCancel.WaitOne(0))
                            return false;
                    }
                }
            }

            return true;
        }

        private bool loadItemData()
        {
            string strType = "Items";
            string strFile = m_rgFiles[strType];
            string strLine = null;

            FileInfo fi = new FileInfo(strFile);
            long lRead = 0;
            Stopwatch sw = new Stopwatch();

            sw.Start();

            using (StreamReader sr = new StreamReader(strFile))
            {
                strLine = sr.ReadLine();
                strLine = sr.ReadLine();

                while (!string.IsNullOrEmpty(strLine))
                {
                    lRead += strLine.Length + 2;

                    string[] rgstr = csvSplit(strLine);
                    ItemRecord rec = new ItemRecord(rgstr);
                    m_rgItems.Add(rec.ItemNum, rec);
                    strLine = sr.ReadLine();

                    if (sw.Elapsed.TotalMilliseconds > 1000)
                    {
                        double dfPct = (double)lRead / (double)fi.Length;
                        m_log.WriteLine("Loading " + strType + " data, " + dfPct.ToString("P4") + " complete.", true);
                        sw.Restart();

                        if (m_evtCancel.WaitOne(0))
                            return false;
                    }
                }
            }

            return true;
        }

        private string[] csvSplit(string str)
        {
            List<string> rgstr = new List<string>();
            string str1 = "";
            int i = 0;

            while (i < str.Length)
            {
                if (str[i] == '"')
                {
                    i++;
                    while (i < str.Length && str[i] != '"')
                    {
                        str1 += str[i];
                        i++;
                    }
                    if (i < str.Length)
                        i++;
                }
                else if (str[i] == ',')
                {
                    rgstr.Add(str1);
                    str1 = "";
                    i++;
                }
                else
                {
                    str1 += str[i];
                    i++;
                }   
            }

            rgstr.Add(str1);

            return rgstr.ToArray();
        }

        private bool loadHolidayData(DateTime dtStart, DateTime dtEnd)
        {
            string strType = "Holidays";
            string strFile = m_rgFiles[strType];
            string strLine = null;

            FileInfo fi = new FileInfo(strFile);
            long lRead = 0;
            Stopwatch sw = new Stopwatch();

            sw.Start();

            using (StreamReader sr = new StreamReader(strFile))
            {
                strLine = sr.ReadLine();
                strLine = sr.ReadLine();

                while (!string.IsNullOrEmpty(strLine))
                {
                    lRead += strLine.Length + 2;

                    string[] rgstr = strLine.Split(',');
                    HolidayRecord rec = HolidayRecord.Parse(rgstr, dtStart, dtEnd);
                    if (rec != null)
                    {
                        if (rec.HolidayLocaleName == "National" && !m_rgHolidaysNational.ContainsKey(rec.Date))
                            m_rgHolidaysNational.Add(rec.Date, rec);
                        else if (rec.HolidayLocaleName == "Regional" && !m_rgHolidaysRegional.ContainsKey(rec.Date))
                            m_rgHolidaysRegional.Add(rec.Date, rec);
                        else if (rec.HolidayLocaleName == "Local" && !m_rgHolidaysLocal.ContainsKey(rec.Date))
                            m_rgHolidaysLocal.Add(rec.Date, rec);
                    }
                    strLine = sr.ReadLine();

                    if (sw.Elapsed.TotalMilliseconds > 1000)
                    {
                        double dfPct = (double)lRead / (double)fi.Length;
                        m_log.WriteLine("Loading " + strType + " data, " + dfPct.ToString("P4") + " complete.", true);
                        sw.Restart();

                        if (m_evtCancel.WaitOne(0))
                            return false;
                    }
                }
            }

            return true;
        }

        private bool loadOilData(DateTime dtStart, DateTime dtEnd)
        {
            string strType = "Oil";
            string strFile = m_rgFiles[strType];
            string strLine = null;

            FileInfo fi = new FileInfo(strFile);
            long lRead = 0;
            Stopwatch sw = new Stopwatch();

            sw.Start();

            using (StreamReader sr = new StreamReader(strFile))
            {
                strLine = sr.ReadLine();
                strLine = sr.ReadLine();

                while (!string.IsNullOrEmpty(strLine))
                {
                    lRead += strLine.Length + 2;

                    string[] rgstr = strLine.Split(',');
                    if (rgstr.Length == 2)
                    {
                        DateTime dt = DateTime.Parse(rgstr[0]);

                        if (dt >= dtStart && dt < dtEnd && !string.IsNullOrEmpty(rgstr[1]))
                        {
                            float fPrice = float.Parse(rgstr[1]);
                            m_rgOil.Add(dt, fPrice);
                        }
                    }

                    strLine = sr.ReadLine();

                    if (sw.Elapsed.TotalMilliseconds > 1000)
                    {
                        double dfPct = (double)lRead / (double)fi.Length;
                        m_log.WriteLine("Loading " + strType + " data, " + dfPct.ToString("P4") + " complete.", true);
                        sw.Restart();

                        if (m_evtCancel.WaitOne(0))
                            return false;
                    }
                }
            }

            return true;
        }

        private bool loadTransactionData(DateTime dtStart, DateTime dtEnd)
        {
            string strType = "Transactions";
            string strFile = m_rgFiles[strType];
            string strLine = null;

            FileInfo fi = new FileInfo(strFile);
            long lRead = 0;
            Stopwatch sw = new Stopwatch();

            sw.Start();

            using (StreamReader sr = new StreamReader(strFile))
            {
                strLine = sr.ReadLine();
                strLine = sr.ReadLine();

                while (!string.IsNullOrEmpty(strLine))
                {
                    lRead += strLine.Length + 2;

                    string[] rgstr = strLine.Split(',');
                    TransactionRecord rec = TransactionRecord.Parse(rgstr, dtStart, dtEnd);
                    if (rec != null)
                    {
                        if (!m_rgTrx.ContainsKey(rec.StoreNum))
                            m_rgTrx.Add(rec.StoreNum, new Dictionary<DateTime, TransactionRecord>());

                        m_rgTrx[rec.StoreNum].Add(rec.Date, rec);
                    }
                    strLine = sr.ReadLine();

                    if (sw.Elapsed.TotalMilliseconds > 1000)
                    {
                        double dfPct = (double)lRead / (double)fi.Length;
                        m_log.WriteLine("Loading " + strType + " data, " + dfPct.ToString("P4") + " complete.", true);
                        sw.Restart();

                        if (m_evtCancel.WaitOne(0))
                            return false;
                    }
                }
            }

            return true;
        }

        public bool SaveAsNumpy(string strPath)
        {
            int nPastSteps = 90;
            int nFutureSteps = 30;
            float fTrainPct = 0.8f;
            float fTestPct = 0.15f;
            float fValdPct = 0.05f;
            int[] rgStaticCat;
            int[] rgStaticCatShape;
            float[] rgStaticNum;
            int[] rgStaticNumShape;
            int[] rgHistCat;
            int[] rgHistCatShape;
            float[] rgHistNum;
            int[] rgHistNumShape;
            int[] rgFutCat;
            int[] rgFutCatShape;
            float[] rgFutNum;
            int[] rgFutNumShape;
            float[] rgTarget;
            int[] rgTargetShape;
            long[] rgTimeIdx;
            int[] rgTimeIdxShape;

            if (!gatherData(nPastSteps, nFutureSteps,
                            out rgStaticCat, out rgStaticCatShape, out rgStaticNum, out rgStaticNumShape,
                            out rgHistCat, out rgHistCatShape, out rgHistNum, out rgHistNumShape,
                            out rgFutCat, out rgFutCatShape, out rgFutNum, out rgFutNumShape,
                            out rgTimeIdx, out rgTimeIdxShape,
                            out rgTarget, out rgTargetShape))
                return false;

            int nStartIdx = 0;
            int nCount = (int)(m_rgRecords.IndexCount * fTrainPct);

            save(strPath, "train", "static_feats", nStartIdx, nCount, rgStaticCat, rgStaticCatShape, rgStaticNum, rgStaticNumShape);
            save(strPath, "train", "historical_ts", nStartIdx, nCount, rgHistCat, rgHistCatShape, rgHistNum, rgHistNumShape);
            save(strPath, "train", "future_ts", nStartIdx, nCount, rgFutCat, rgFutCatShape, rgFutNum, rgFutNumShape);
            save(strPath, "train", "time_index", nStartIdx, nCount, rgTimeIdx, rgTimeIdxShape);

            nStartIdx = nCount;
            nCount = (int)(m_rgRecords.IndexCount * fTestPct);

            save(strPath, "test", "static_feats", nStartIdx, nCount, rgStaticCat, rgStaticCatShape, rgStaticNum, rgStaticNumShape);
            save(strPath, "test", "historical_ts", nStartIdx, nCount, rgHistCat, rgHistCatShape, rgHistNum, rgHistNumShape);
            save(strPath, "test", "future_ts", nStartIdx, nCount, rgFutCat, rgFutCatShape, rgFutNum, rgFutNumShape);
            save(strPath, "test", "time_index", nStartIdx, nCount, rgTimeIdx, rgTimeIdxShape);

            nStartIdx += nCount;
            nCount = m_rgRecords.IndexCount - nStartIdx;

            save(strPath, "validation", "static_feats", nStartIdx, nCount, rgStaticCat, rgStaticCatShape, rgStaticNum, rgStaticNumShape);
            save(strPath, "validation", "historical_ts", nStartIdx, nCount, rgHistCat, rgHistCatShape, rgHistNum, rgHistNumShape);
            save(strPath, "validation", "future_ts", nStartIdx, nCount, rgFutCat, rgFutCatShape, rgFutNum, rgFutNumShape);
            save(strPath, "validation", "time_index", nStartIdx, nCount, rgTimeIdx, rgTimeIdxShape);

            return true;
        }

        private bool gatherData(int nPastSteps,
                                int nFutureSteps,
                                out int[] rgStaticCategorical, out int[] rgStaticCategoricalShape, out float[] rgStaticNumeric, out int[] rgStaticNumericShape,
                                out int[] rgHistoricalCategorical, out int[] rgHistoricalCategoricalShape, out float[] rgHistoricalNumeric, out int[] rgHistoricalNumericShape,
                                out int[] rgFutureCategorical, out int[] rgFutureCategoricalShape, out float[] rgFutureNumeric, out int[] rgFutureNumericShape,
                                out long[] rgTimeIdx, out int[] rgTimeIdxShape,
                                out float[] rgTarget, out int[] rgTargetShape)
        {
            m_rgRecords.Reset();

            int nCount = m_rgRecords.IndexCount;

            int nNumStaticCategorical = DataRecord.NumStaticCategorical;
            int nNumStaticNumeric = DataRecord.NumStaticNumeric;
            int nNumHistoricalCategorical = DataRecord.NumHistoricalCategorical;
            int nNumHistoricalNumeric = DataRecord.NumHistoricalNumeric;
            int nNumFutureCategorical = DataRecord.NumFutureCategorical;
            int nNumFutureNumeric = DataRecord.NumFutureNumeric;

            rgStaticCategorical = null;
            rgStaticNumeric = null;
            rgHistoricalCategorical = null;
            rgHistoricalNumeric = null;
            rgFutureCategorical = null;
            rgFutureNumeric = null;
            rgTimeIdx = null;

            rgStaticCategoricalShape = new int[] { nCount, nNumStaticCategorical };
            rgStaticNumericShape = new int[] { nCount, nNumStaticNumeric };
            rgHistoricalCategoricalShape = new int[] { nCount, nPastSteps, nNumHistoricalCategorical };
            rgHistoricalNumericShape = new int[] { nCount, nPastSteps, nNumHistoricalNumeric };
            rgFutureCategoricalShape = new int[] { nCount, nFutureSteps, nNumFutureCategorical };
            rgFutureNumericShape = new int[] { nCount, nFutureSteps, nNumFutureNumeric };
            rgTargetShape = new int[] { nCount, nFutureSteps };
            rgTimeIdxShape = new int[] { nCount };

            if (nNumStaticCategorical > 0)
                rgStaticCategorical = new int[nCount * nNumStaticCategorical];

            if (nNumStaticNumeric > 0)
                rgStaticNumeric = new float[nCount * nNumStaticNumeric];

            if (nNumHistoricalCategorical > 0)
                rgHistoricalCategorical = new int[nCount * nPastSteps * nNumHistoricalCategorical];

            if (nNumHistoricalNumeric > 0)
                rgHistoricalNumeric = new float[nCount * nPastSteps * nNumHistoricalNumeric];

            if (nNumFutureCategorical > 0)
                rgFutureCategorical = new int[nCount * nFutureSteps * nNumFutureCategorical];

            if (nNumFutureNumeric > 0)
                rgFutureNumeric = new float[nCount * nFutureSteps * nNumFutureNumeric];

            rgTimeIdx = new long[nCount];
            rgTarget = new float[nCount * nFutureSteps];

            Stopwatch sw = new Stopwatch();
            DataRecord rec = m_rgRecords.Next();
            int nIdx = 0;

            int nStatCatOffset = 0;
            int nStatNumOffset = 0;
            int nHistCatOffset = 0;
            int nHistNumOffset = 0;
            int nFutCatOffset = 0;
            int nFutNumOffset = 0;
            int nTargetOffset = 0;

            sw.Start();
            while (rec != null)
            {
                rgTimeIdx[nIdx] = rec.UnixTime;

                //nStatCatOffset = rec.LoadStaticCategorical(rgStaticCategorical, nStatCatOffset);
                //nStatNumOffset = rec.LoadStaticNumeric(rgStaticNumeric, nStatNumOffset);
                //nHistCatOffset = rec.LoadHistoricalCategorical(rgHistoricalCategorical, nHistCatOffset);
                //nHistNumOffset = rec.LoadHistoricalNumeric(rgHistoricalNumeric, nHistNumOffset);
                //nFutCatOffset = rec.LoadFutureCategorical(rgFutureCategorical, nFutCatOffset);
                //nFutNumOffset = rec.LoadFutureNumeric(rgFutureNumeric, nFutNumOffset);
                //nTargetOffset = rec.LoadTarget(rgTarget, nTargetOffset);

                if (sw.Elapsed.TotalMilliseconds > 1000)
                {
                    double dfPct = (double)nIdx / m_rgRecords.IndexCount;
                    m_log.WriteLine("Gathering data at " + dfPct.ToString("P") + " complete.");
                    sw.Restart();

                    if (m_evtCancel.WaitOne(0))
                        return false;
                }

                nIdx++;
            }

            return true;
        }

        private void save(string strPath, string strUse, string strType, int nStartIdx, int nCount, int[] rgCat, int[] rgCatShape, float[] rgNum, int[] rgNumShape)
        {
            int[] rgCat1 = null;
            int[] rgNum1 = null;

            if (rgCatShape.Last() > 0)
            {
                int nItemCount = nCount;
                int nSpatialDim = 1;
                for (int i = 1; i < rgCatShape.Length; i++)
                {
                    nSpatialDim *= rgCatShape[i];
                }

                rgCat1 = new int[nItemCount * nSpatialDim];

                Array.Copy(rgCat, 0, rgCat1, nStartIdx * nSpatialDim, nCount * nSpatialDim);
            }

            if (rgNumShape != null && rgNumShape.Last() > 0)
            {
                int nItemCount = nCount;
                int nSpatialDim = 1;
                for (int i = 1; i < rgNumShape.Length; i++)
                {
                    nSpatialDim *= rgNumShape[i];
                }

                rgNum1 = new int[nItemCount * nSpatialDim];

                Array.Copy(rgNum, 0, rgNum1, nStartIdx * nSpatialDim, nCount * nSpatialDim);
            }

            rgCatShape[0] = nCount;
            Blob<float>.SaveToNumpy(strPath + strUse + "_" + strType + "_categorical.npy", rgCat1, rgCatShape);

            if (rgNumShape != null)
            {
                rgNumShape[0] = nCount;
                Blob<float>.SaveToNumpy(strPath + strUse + "_" + strType + "_numerical.npy", rgNum1, rgNumShape);
            }
        }

        private void save(string strPath, string strUse, string strType, int nStartIdx, int nCount, long[] rgTime, int[] rgTimeShape)
        {
            int[] rgTime1 = null;

            if (rgTimeShape.Last() > 0)
            {
                int nItemCount = nCount;
                int nSpatialDim = 1;

                rgTime1 = new int[nItemCount * nSpatialDim];

                Array.Copy(rgTime, 0, rgTime1, nStartIdx * nSpatialDim, nCount * nSpatialDim);
            }

            rgTimeShape[0] = nCount;
            Blob<float>.SaveToNumpy(strPath + strUse + "_" + strType + "_categorical.npy", rgTime, rgTimeShape);
        }

        public bool SaveData(string strFile)
        {
            Stopwatch sw1 = new Stopwatch();

            sw1.Start();

            int nIdx = 0;
            int nCount = m_rgRecords.Count;
            m_rgRecords.Reset();

            using (StreamWriter sw = new StreamWriter(strFile))
            {
                sw.WriteLine(DataRecord.GetHeader());

                DataRecord rec = m_rgRecords.Next();
                while (rec != null)
                {
                    string strLine = rec.ToString(nIdx, m_rgStores, m_rgItems, m_rgHolidaysLocal, m_rgHolidaysRegional, m_rgHolidaysNational);
                    sw.WriteLine(strLine);
                    rec = m_rgRecords.Next();
                    nIdx++;

                    if (sw1.Elapsed.TotalMilliseconds > 1000)
                    {
                        double dfPct = (double)nIdx / (double)nCount;
                        m_log.WriteLine("Saving data, " + dfPct.ToString("P") + " complete.", true);
                        sw1.Restart();

                        if (m_evtCancel.WaitOne(0))
                            return false;
                    }
                }
            }

            return true;    
        }
    }

    public class DataRecordCollection
    {
        Dictionary<int, Dictionary<int, Dictionary<DateTime, DataRecord>>> m_rgRecords = new Dictionary<int, Dictionary<int, Dictionary<DateTime, DataRecord>>>();
        Dictionary<DateTime, List<DataRecord>> m_rgRecordsByDate = new Dictionary<DateTime, List<DataRecord>>();
        Dictionary<int, List<DataRecord>> m_rgRecordsByStore = new Dictionary<int, List<DataRecord>>();
        Dictionary<int, List<DataRecord>> m_rgRecordsByItem = new Dictionary<int, List<DataRecord>>();
        List<Tuple<int, int, DateTime>> m_rgIndex = new List<Tuple<int, int, DateTime>>();
        int m_nIdx = 0;
        int m_nCount;
        Log m_log;
        CancelEvent m_evtCancel;
        Stopwatch m_sw = new Stopwatch();

        int m_nStoreIdx = 0;
        int m_nItemIdx = 0;
        int m_nDateIdx = 0;

        public DataRecordCollection(Log log, CancelEvent evtCancel)
        {
            m_log = log;
            m_evtCancel = evtCancel;
        }

        public bool Reset(bool bRebuildIndex = true)
        {
            m_nIdx = 0;

            if (bRebuildIndex || m_rgIndex.Count == 0)
            {
                int nIdx = 0;

                m_sw.Restart();
                m_rgIndex.Clear();

                foreach (KeyValuePair<int, Dictionary<int, Dictionary<DateTime, DataRecord>>> kv in m_rgRecords)
                {
                    foreach (KeyValuePair<int, Dictionary<DateTime, DataRecord>> kv2 in kv.Value)
                    {
                        foreach (KeyValuePair<DateTime, DataRecord> kv3 in kv2.Value)
                        {
                            m_rgIndex.Add(new Tuple<int, int, DateTime>(kv.Key, kv2.Key, kv3.Key));

                            if (m_sw.Elapsed.TotalMilliseconds > 1000)
                            {
                                double dfPct = (double)nIdx / (double)m_nCount;
                                m_log.WriteLine("Indexing at " + dfPct.ToString("P") + "...");
                                m_sw.Restart();

                                if (m_evtCancel.WaitOne(0))
                                    return false;
                            }
                        }
                    }
                }
            }

            return true;
        }

        public int IndexCount
        {
            get { return m_rgIndex.Count; }
        }

        public DataRecord Next()
        {
            if (m_nIdx == m_rgIndex.Count)
                return null;

            int nStoreNbr = m_rgIndex[m_nIdx].Item1;
            int nItemNbr = m_rgIndex[m_nIdx].Item2;
            DateTime dt = m_rgIndex[m_nIdx].Item3;

            m_nIdx++;

            return m_rgRecords[nStoreNbr][nItemNbr][dt];
        }

        public void Clear()
        {
            m_rgRecords.Clear();
            m_rgRecordsByDate.Clear();
            m_rgRecordsByStore.Clear();
            m_rgRecordsByItem.Clear();
        }

        public void Add(DataRecord item)
        {
            if (!m_rgRecords.ContainsKey(item.StoreNum))
                m_rgRecords.Add(item.StoreNum, new Dictionary<int, Dictionary<DateTime, DataRecord>>());

            if (!m_rgRecords[item.StoreNum].ContainsKey(item.ItemNum))
                m_rgRecords[item.StoreNum].Add(item.ItemNum, new Dictionary<DateTime, DataRecord>());

            if (!m_rgRecords[item.StoreNum][item.ItemNum].ContainsKey(item.Date))
                m_rgRecords[item.StoreNum][item.ItemNum].Add(item.Date, item);

            if (m_rgRecordsByDate.ContainsKey(item.Date))
                m_rgRecordsByDate[item.Date].Add(item);
            else
                m_rgRecordsByDate.Add(item.Date, new List<DataRecord>() { item });

            if (m_rgRecordsByStore.ContainsKey(item.StoreNum))
                m_rgRecordsByStore[item.StoreNum].Add(item);
            else
                m_rgRecordsByStore.Add(item.StoreNum, new List<DataRecord>() { item });

            if (m_rgRecordsByItem.ContainsKey(item.ItemNum))
                m_rgRecordsByItem[item.ItemNum].Add(item);
            else
                m_rgRecordsByItem.Add(item.ItemNum, new List<DataRecord>() { item });

            m_nCount++;
        }

        public bool AddOil(Dictionary<DateTime, float> rgOil)
        {
            m_sw.Restart();
            int nIdx = 0;

            foreach (KeyValuePair<DateTime, float> kv in rgOil)
            {
                if (m_rgRecordsByDate.ContainsKey(kv.Key))
                {
                    foreach (DataRecord rec in m_rgRecordsByDate[kv.Key])
                    {
                        rec.OilPrice = kv.Value;
                        nIdx++;

                        if (m_sw.Elapsed.TotalMilliseconds > 1000)
                        {
                            if (m_evtCancel.WaitOne(0))
                                return false;

                            double dfPct = (double)nIdx / (double)rgOil.Count;
                            m_log.WriteLine("Adding store information to data records - " + dfPct.ToString("P") + " complete.");
                            m_sw.Restart();
                        }
                    }
                }
            }

            return true;
        }

        public bool AddStoreInfo(Dictionary<int, StoreRecord> rgStores)
        {
            m_sw.Restart();
            int nIdx = 0;

            foreach (KeyValuePair<int, List<DataRecord>> kv in m_rgRecordsByStore)
            {
                if (!rgStores.ContainsKey(kv.Key))
                    throw new Exception("The store '" + kv.Key.ToString() + "' was not found in the store information.");

                StoreRecord store = rgStores[kv.Key];

                foreach (DataRecord rec in kv.Value)
                {
                    rec.AddStoreInfo(store);
                    nIdx++;

                    if (m_sw.Elapsed.TotalMilliseconds > 1000)
                    {
                        if (m_evtCancel.WaitOne(0))
                            return false;

                        double dfPct = (double)nIdx / (double)m_nCount;
                        m_log.WriteLine("Adding store information to data records - " + dfPct.ToString("P") + " complete.");
                        m_sw.Restart();
                    }
                }
            }

            return true;
        }

        public bool AddItemInfo(Dictionary<int, ItemRecord> rgItems)
        {
            m_sw.Restart();
            int nIdx = 0;

            foreach (KeyValuePair<int, List<DataRecord>> kv in m_rgRecordsByItem)
            {
                if (!rgItems.ContainsKey(kv.Key))
                    throw new Exception("The item '" + kv.Key.ToString() + "' was not found in the item information.");

                ItemRecord item = rgItems[kv.Key];

                foreach (DataRecord rec in kv.Value)
                {
                    rec.AddItemInfo(item);
                    nIdx++;

                    if (m_sw.Elapsed.TotalMilliseconds > 1000)
                    {
                        if (m_evtCancel.WaitOne(0))
                            return false;

                        double dfPct = (double)nIdx / (double)m_nCount;
                        m_log.WriteLine("Adding item information to data records - " + dfPct.ToString("P") + " complete.");
                        m_sw.Restart();
                    }
                }
            }

            return true;
        }

        public bool AddHolidayInfo(Dictionary<DateTime, HolidayRecord> rgHolidays)
        {
            m_sw.Restart();
            int nIdx = 0;

            foreach (KeyValuePair<DateTime, HolidayRecord> kvp in rgHolidays)
            {
                if (m_rgRecordsByDate.ContainsKey(kvp.Key))
                {
                    foreach (DataRecord rec in m_rgRecordsByDate[kvp.Key])
                    {
                        rec.AddHolidayInfo(kvp.Value);
                    }
                }

                nIdx++;

                if (m_sw.Elapsed.TotalMilliseconds > 1000)
                {
                    if (m_evtCancel.WaitOne(0))
                        return false;

                    double dfPct = (double)nIdx / (double)rgHolidays.Count;
                    m_log.WriteLine("Adding holiday information to data records - " + dfPct.ToString("P") + " complete.");
                    m_sw.Restart();
                }
            }

            return true;
        }

        public bool AddTransactionInfo(Dictionary<int, Dictionary<DateTime, TransactionRecord>> rgTrx)
        {
            m_sw.Restart();
            int nIdx = 0;
            int nCount = rgTrx.Sum(p => p.Value.Count);

            foreach (KeyValuePair<int, Dictionary<DateTime, TransactionRecord>> kvp in rgTrx)
            {
                foreach (KeyValuePair<DateTime, TransactionRecord> kvp2 in kvp.Value)
                {
                    if (m_rgRecordsByDate.ContainsKey(kvp2.Key))
                    {
                        foreach (DataRecord rec in m_rgRecordsByDate[kvp2.Key].Where(p => p.StoreNum == kvp2.Value.StoreNum))
                        {
                            rec.AddTransactionInfo(kvp2.Value);
                        }
                    }

                    nIdx++;
                }

                if (m_sw.Elapsed.TotalMilliseconds > 1000)
                {
                    if (m_evtCancel.WaitOne(0))
                        return false;

                    double dfPct = (double)nIdx / (double)nCount;
                    m_log.WriteLine("Adding transaction information to data records - " + dfPct.ToString("P") + " complete.");
                    m_sw.Restart();
                }
            }

            return true;
        }

        public int Count
        {
            get { return m_nCount; }
        }
    }

    public class DataRecord
    {
        int m_nID;
        DateTime m_dt;                  // 0 hist/fut num (unix time)
        // day of week                  // 1 hist/fut num 
        // day of month                 // 2 hist/fut num
        // month                        // 3 hist/fut num

        int m_nStoreNum;                // static cat 0
        int m_nStoreCityID;             // static cat 1
        int m_nStoreStateID;            // static cat 2
        int m_nStoreTypeID;             // static cat 3
        int m_nStoreCluster;            // static cat 4
        int m_nStoreTransactions;       // hist num 0

        int m_nItemNum;                 // static cat 5
        int m_nItemFamilyID;            // static cat 6
        int m_nItemClassID;             // static cat 7
        int m_nItemPerishable;          // static cat 8

        float m_fUnitSales;         
        float m_fLogSales;              // 4 past num

        int m_nOnPromotion;             // static cat 9

        float m_fOilPrice;              // 5 hist num

        int m_nHolidayType;             // 0 hist cat
        int m_nHolidayLocaleID;         // 1 hist cat

        public DataRecord(DateTime dt, string[] rgstr)
        {
            m_nID = int.Parse(rgstr[0]);
            m_dt = dt;
            m_nStoreNum = int.Parse(rgstr[2]);
            m_nItemNum = int.Parse(rgstr[3]);
            m_fUnitSales = float.Parse(rgstr[4]);
            m_fLogSales = (float)Math.Log(m_fUnitSales);

            m_nOnPromotion = 0;
            if (rgstr[5].ToLower() == "true")
                m_nOnPromotion = 1;
        }

        public static DataRecord Parse(string[] rgstr, DateTime dtStart, DateTime dtEnd)
        {
            if (rgstr.Length != 6)
                throw new Exception("The store record must have 6 fields.");

            DateTime dt = DateTime.Parse(rgstr[1]);
            if (dt < dtStart || dt > dtEnd)
                return null;

            return new DataRecord(dt, rgstr);
        }

        public static string GetHeader()
        {
            return "id,store_nbr,item_nbr,unit_sales,onpromotion,traj_id,unique_id,open,date,log_sales,city,state,type,cluster,family,class,perishable,transaction,day_of_week,day_of_month,month,national_hol,regional_hol,local_hol";
        }

        public static int NumStaticCategorical
        {
            get { return 10;  }
        }

        public static int NumStaticNumeric
        {
            get { return 0; }
        }

        public static int NumHistoricalCategorical
        {
            get { return 2; }
        }

        public static int NumHistoricalNumeric
        {
            get { return 6; }
        }

        public static int NumFutureCategorical
        {
            get { return 0; }
        }

        public static int NumFutureNumeric
        {
            get { return 0; }
        }

        public string ToString(int nIdx, Dictionary<int, StoreRecord> rgStores, Dictionary<int, ItemRecord> rgItems, Dictionary<DateTime, HolidayRecord> rgHolidaysLocal, Dictionary<DateTime, HolidayRecord> rgHolidaysRegional, Dictionary<DateTime, HolidayRecord> rgHolidaysNational)
        {
            StringBuilder sb = new StringBuilder();
            string strStoreNum = m_nStoreNum.ToString();
            string strItemNum = m_nItemNum.ToString();
            string strTrajID = strStoreNum + "_" + strItemNum;
            string strDate = m_dt.ToString("yyyy-MM-dd");
            string strUniqueID = strTrajID + "_" + strDate;
            StoreRecord store = rgStores[m_nStoreNum];
            ItemRecord item = rgItems[m_nItemNum];
            HolidayRecord holidayLocal = null;
            HolidayRecord holidayRegional = null;
            HolidayRecord holidayNational = null;

            if (rgHolidaysLocal.ContainsKey(m_dt))
                holidayLocal = rgHolidaysLocal[m_dt];
            if (rgHolidaysRegional.ContainsKey(m_dt))
                holidayRegional = rgHolidaysRegional[m_dt];
            if (rgHolidaysNational.ContainsKey(m_dt))
                holidayNational = rgHolidaysNational[m_dt];

            sb.Append(nIdx.ToString());
            sb.Append(',');
            sb.Append(strStoreNum);
            sb.Append(',');
            sb.Append(strItemNum);
            sb.Append(',');
            sb.Append(m_fUnitSales.ToString());
            sb.Append(',');
            sb.Append(m_nOnPromotion == 0 ? "FALSE" : "TRUE"); 
            sb.Append(',');
            sb.Append(strTrajID);
            sb.Append(',');
            sb.Append(strUniqueID);
            sb.Append(',');
            sb.Append("1,");
            sb.Append(strDate);
            sb.Append(',');
            sb.Append(m_fLogSales.ToString());
            sb.Append(',');
            sb.Append(store.City);
            sb.Append(',');
            sb.Append(store.State);
            sb.Append(',');
            sb.Append(store.Type);
            sb.Append(',');
            sb.Append(store.Cluster.ToString());
            sb.Append(',');
            sb.Append(item.Family);
            sb.Append(',');
            sb.Append(item.Class);
            sb.Append(',');
            sb.Append(item.Perishable.ToString());
            sb.Append(',');
            sb.Append(m_nStoreTransactions.ToString());
            sb.Append(',');
            sb.Append(((int)m_dt.DayOfWeek).ToString());
            sb.Append(',');
            sb.Append(m_dt.Day.ToString());
            sb.Append(',');
            sb.Append(m_dt.Month.ToString());
            sb.Append(',');

            if (holidayNational != null)
            {
                sb.Append(holidayNational.Description);
                sb.Append(',');
                sb.Append(',');
            }
            else if (holidayRegional != null)
            {
                sb.Append(',');
                sb.Append(holidayRegional.Description);
                sb.Append(',');
            }
            else if (holidayLocal != null)
            {
                sb.Append(',');
                sb.Append(',');
                sb.Append(holidayLocal.Description);
            }
            else
            {
                sb.Append(',');
                sb.Append(',');
            }

            return sb.ToString();
        }

        public void AddStoreInfo(StoreRecord store)
        {
            if (m_nStoreNum != store.StoreNum)
                throw new Exception("Store number mismatch.");

            m_nStoreCityID = store.CityID;
            m_nStoreStateID = store.StateID;
            m_nStoreTypeID = store.TypeID;
            m_nStoreCluster = store.Cluster;
        }

        public void AddItemInfo(ItemRecord item)
        {
            if (m_nItemNum != item.ItemNum)
                throw new Exception("Item number mismatch.");

            m_nItemFamilyID = item.FamilyID;
            m_nItemClassID = item.ClassID;
            m_nItemPerishable = item.Perishable;
        }

        public void AddHolidayInfo(HolidayRecord holiday)
        {
            m_nHolidayType = 0;
            m_nHolidayLocaleID = 0;

            if (m_dt == holiday.Date)
            { 
                m_nHolidayType = holiday.HolidayType;
                m_nHolidayLocaleID = holiday.HolidayLocaleID;
            }
        }

        public void AddTransactionInfo(TransactionRecord trx)
        {
            if (m_nStoreNum == trx.StoreNum)
                m_nStoreTransactions = trx.TransactionCount;
        }

        public int ID
        {
            get { return m_nID; }
        }

        public DateTime Date
        {
            get { return m_dt; }
        }

        public long UnixTime
        {
            get { return new DateTimeOffset(m_dt).ToUnixTimeSeconds(); }
        }

        public int StoreNum
        {
            get { return m_nStoreNum; }
        }

        public int StoreCityID
        {
            get { return m_nStoreCityID; }
        }

        public int StoreStateID
        {
            get { return m_nStoreStateID; }
        }

        public int StoreTypeID
        {
            get { return m_nStoreTypeID; }
        }

        public int StoreCluster
        {
            get { return m_nStoreCluster; }
        }

        public int ItemNum
        {
            get { return m_nItemNum; }
        }

        public int ItemFamilyID
        {
            get { return m_nItemFamilyID; }
        }

        public int ItemClassID
        {
            get { return m_nItemClassID; }
        }

        public int ItemPerishable
        {
            get { return m_nItemPerishable; }
        }

        public float UnitSales
        {
            get { return m_fUnitSales; }
        }

        public float LogSales
        {
            get { return m_fLogSales; }
        }

        public int OnPromotion
        {
            get { return m_nOnPromotion; }
        }

        public float OilPrice
        {
            get { return m_fOilPrice; }
            set { m_fOilPrice = value; }
        }

        public int HolidayType
        {
            get { return m_nHolidayType; }
        }

        public int HolidayLocaleID
        {
            get { return m_nHolidayLocaleID; }
        }
    }

    public class ItemRecord
    {
        static Dictionary<string, int> m_rgItemFamily = new Dictionary<string, int>();
        static Dictionary<string, int> m_rgItemClass = new Dictionary<string, int>();

        int m_nItemNum;
        int m_nFamilyID;
        int m_nItemClassID;
        int m_nPerishable;

        public ItemRecord(string[] rgstr)
        {
            if (rgstr.Length != 4)
                throw new Exception("The item record must have 4 fields.");

            m_nItemNum = int.Parse(rgstr[0]);

            string strFamily = rgstr[1];
            if (!m_rgItemFamily.ContainsKey(strFamily))
                m_rgItemFamily.Add(strFamily, m_rgItemFamily.Count);

            m_nFamilyID = m_rgItemFamily[strFamily];

            string strClass = rgstr[2];
            if (!m_rgItemClass.ContainsKey(strClass))
                m_rgItemClass.Add(strClass, m_rgItemClass.Count);

            m_nItemClassID = m_rgItemClass[strClass];

            m_nPerishable = int.Parse(rgstr[3]);
        }

        public int ItemNum
        {
            get { return m_nItemNum; }
        }

        public int FamilyID
        {
            get { return m_nFamilyID; }
        }

        public string Family
        {
            get { return m_rgItemFamily.FirstOrDefault(p => p.Value == m_nFamilyID).Key; }
        }

        public int ClassID
        {
            get { return m_nItemClassID; }
        }

        public string Class
        {
            get { return m_rgItemClass.FirstOrDefault(p => p.Value == m_nItemClassID).Key; }
        }

        public int Perishable
        {
            get { return m_nPerishable; }
        }
    }

    public class StoreRecord
    {
        static Dictionary<string, int> m_rgStoreCity = new Dictionary<string, int>();
        static Dictionary<string, int> m_rgStoreState = new Dictionary<string, int>();
        static Dictionary<string, int> m_rgStoreType = new Dictionary<string, int>();

        int m_nStoreNum;
        int m_nCityID;
        int m_nStateID;
        int m_nTypeID;
        int m_nCluster;

        public StoreRecord(string[] rgstr)
        {
            if (rgstr.Length != 5)
                throw new Exception("The store record must have 5 fields.");

            m_nStoreNum = int.Parse(rgstr[0]);

            string strCity = rgstr[1];
            if (!m_rgStoreCity.ContainsKey(strCity))
                m_rgStoreCity.Add(strCity, m_rgStoreCity.Count);
            
            m_nCityID = m_rgStoreCity[strCity];

            string strState = rgstr[2];
            if (!m_rgStoreState.ContainsKey(strState))
                m_rgStoreState.Add(strState, m_rgStoreState.Count);

            m_nStateID = m_rgStoreState[strState];

            string strType = rgstr[3];
            if (!m_rgStoreType.ContainsKey(strType))
                m_rgStoreType.Add(strType, m_rgStoreType.Count);

            m_nTypeID = m_rgStoreType[strType];
            m_nCluster = int.Parse(rgstr[4]);
        }

        public int StoreNum
        {
            get { return m_nStoreNum; }
        }

        public int CityID
        {
            get { return m_nCityID; }
        }

        public string City
        {
            get { return m_rgStoreCity.FirstOrDefault(p => p.Value == m_nCityID).Key; }
        }

        public int StateID
        {
            get { return m_nStateID; }
        }

        public string State
        {
            get { return m_rgStoreState.FirstOrDefault(p => p.Value == m_nStateID).Key; }
        }

        public int TypeID
        {
            get { return m_nTypeID; }
        }

        public string Type
        {
            get { return m_rgStoreType.FirstOrDefault(p => p.Value == m_nTypeID).Key; }
        }

        public int Cluster
        {
            get { return m_nCluster; }
        }
    }

    public class TransactionRecord
    {
        DateTime m_dt;
        int m_nStoreNum;
        int m_nTransactionCount;

        public TransactionRecord(DateTime dt, string[] rgstr)
        {
            m_dt = dt;
            m_nStoreNum = int.Parse(rgstr[1]);
            m_nTransactionCount = int.Parse(rgstr[2]);
        }

        public static TransactionRecord Parse(string[] rgstr, DateTime dtStart, DateTime dtEnd)
        {
            if (rgstr.Length != 3)
                throw new Exception("The transaction record must have 3 fields.");

            DateTime dt = DateTime.Parse(rgstr[0]);
            if (dt < dtStart || dt > dtEnd)
                return null;

            return new TransactionRecord(dt, rgstr);
        }

        public DateTime Date
        {
            get { return m_dt; }
        }

        public int StoreNum
        {
            get { return m_nStoreNum; }
        }

        public int TransactionCount
        {
            get { return m_nTransactionCount; }
        }
    }

    public class HolidayRecord
    {
        static Dictionary<string, int> m_rgHolidayType = new Dictionary<string, int>();
        static Dictionary<string, int> m_rgHolidayLocale = new Dictionary<string, int>();

        DateTime m_dt;
        int m_nHolidayType;
        int m_nHolidayLocaleID;
        string m_strName;
        string m_strDescription;

        public HolidayRecord(DateTime dt, string[] rgstr)
        {
            m_dt = dt;

            try
            {
                string strType = rgstr[1];
                if (!m_rgHolidayType.ContainsKey(strType))
                    m_rgHolidayType.Add(strType, m_rgHolidayType.Count);

                m_nHolidayType = m_rgHolidayType[strType];

                string strLocale = rgstr[2];
                if (!m_rgHolidayLocale.ContainsKey(strLocale))
                    m_rgHolidayLocale.Add(strLocale, m_rgHolidayLocale.Count);

                m_nHolidayLocaleID = m_rgHolidayLocale[strLocale];

                m_strName = rgstr[3];
                m_strDescription = rgstr[4];
            }
            catch (Exception excpt)
            {
                throw excpt;
            }
        }

        public static HolidayRecord Parse(string[] rgstr, DateTime dtStart, DateTime dtEnd)
        {
            if (rgstr.Length != 6)
                throw new Exception("The holiday record must have 6 fields.");

            DateTime dt = DateTime.Parse(rgstr[0]);
            if (dt < dtStart || dt > dtEnd)
                return null;

            return new HolidayRecord(dt, rgstr);
        }

        public DateTime Date
        {
            get { return m_dt; }
        }

        public int HolidayType
        {
            get { return m_nHolidayType; }
        }

        public string HolidayTypeName
        {
            get { return m_rgHolidayType.FirstOrDefault(p => p.Value == m_nHolidayType).Key; }
        }

        public int HolidayLocaleID
        {
            get { return m_nHolidayLocaleID; }
        }

        public string HolidayLocaleName
        {
            get { return m_rgHolidayLocale.FirstOrDefault(p => p.Value == m_nHolidayLocaleID).Key; }
        }

        public string Name
        {
            get { return m_strName; }
        }

        public string Description
        {
            get { return m_strDescription; }
        }
    }
}
