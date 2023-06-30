﻿using MyCaffe.basecode;
using MyCaffe.common;
using MyCaffe.db.temporal;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

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
        Stopwatch m_sw = new Stopwatch();

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
                        m_log.WriteLine("Loading " + strType + " data, " + dfPct.ToString("P") + " complete.", true);
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

        public FavoritaData SplitData(string strName, double dfPctStart, double dfPctEnd)
        {
            FavoritaData data = new FavoritaData(m_rgFiles, m_log, m_evtCancel);

            data.m_rgRecords = m_rgRecords.Split(strName, dfPctStart, dfPctEnd);
            data.m_rgStores = m_rgStores;
            data.m_rgItems = m_rgItems;
            data.m_rgHolidaysLocal = m_rgHolidaysLocal;
            data.m_rgHolidaysNational = m_rgHolidaysNational;
            data.m_rgHolidaysRegional = m_rgHolidaysRegional;
            data.m_rgOil = m_rgOil;
            data.m_rgTrx = m_rgTrx;
            data.m_rgFiles = m_rgFiles;

            return data;
        }

        public bool NormalizeData(Dictionary<int, Dictionary<int, Dictionary<DataRecord.FIELD, Tuple<double, double>>>> rgScalers)
        {
            return m_rgRecords.Normalize(m_sw, m_log, m_evtCancel, rgScalers);
        }

        public int SaveAsSql(string strName, string strSub)
        {
            DatabaseTemporal db = new DatabaseTemporal();
            Stopwatch sw = new Stopwatch();

            sw.Start();

            int nSrcID = db.AddSource(strName + "." + strSub, m_rgRecords.RecordsByStoreItem.Count, 5, m_rgRecords.RecordsByStoreItem.Max(p => p.Value.Count), true, 0, false);
            int nItemCount = m_rgRecords.RecordsByStoreItem.Sum(p => p.Value.Count);
            int nItemIdx = 0;

            db.Open(nSrcID);
            db.EnableBulk(true);

            Dictionary<int, int> rgIdxToItemID = new Dictionary<int, int>();
            List<KeyValuePair<int, ItemRecord>> rgItems1 = m_rgItems.OrderBy(p => p.Key).ToList();
            for (int i=0; i<rgItems1.Count; i++)
            {
                rgIdxToItemID.Add(rgItems1[i].Key, i);
            }

            foreach (KeyValuePair<int, Dictionary<int, List<DataRecord>>> kv in m_rgRecords.RecordsByStoreItem)
            {
                int nStoreID = kv.Key;
                StoreRecord store = m_rgStores[nStoreID];
                string strStore = store.StoreNum.ToString();

                foreach (KeyValuePair<int, List<DataRecord>> kv1 in kv.Value)
                {
                    List<DataRecord> rgItems = kv1.Value.OrderBy(p => p.Date).ToList();
                    int nItemID = rgIdxToItemID[kv1.Key];
                    ItemRecord item = m_rgItems[nItemID];
                    string strItem = item.ItemNum.ToString();
                    string strStoreItem = strStore + "." + strItem;

                    DateTime dtStart = rgItems[0].Date;
                    DateTime dtEnd = rgItems[rgItems.Count - 1].Date;
                    int nSecondsPerStep = 1;

                    int nOrdering = 0;
                    int nStreamID_logsales = db.AddObservedValueStream(nSrcID, nItemIdx, strStoreItem + ".logsales", MyCaffe.basecode.descriptors.ValueStreamDescriptor.STREAM_VALUE_TYPE.NUMERIC, nOrdering++, dtStart, dtEnd, nSecondsPerStep);
                    int nStreamID_oilprice = db.AddObservedValueStream(nSrcID, nItemIdx, "OilPrice", MyCaffe.basecode.descriptors.ValueStreamDescriptor.STREAM_VALUE_TYPE.NUMERIC, nOrdering++, dtStart, dtEnd, nSecondsPerStep);
                    int nStreamID_storetrx = db.AddObservedValueStream(nSrcID, nItemIdx, strStoreItem + ".trx", MyCaffe.basecode.descriptors.ValueStreamDescriptor.STREAM_VALUE_TYPE.NUMERIC, nOrdering++, dtStart, dtEnd, nSecondsPerStep);

                    int nStreamID_onpromotion = db.AddKnownValueStream(nSrcID, nItemIdx, strStoreItem + ".onpromotion", MyCaffe.basecode.descriptors.ValueStreamDescriptor.STREAM_VALUE_TYPE.CATEGORICAL, nOrdering++, dtStart, dtEnd, nSecondsPerStep);
                    int nStreamID_dayofweek = db.AddKnownValueStream(nSrcID, nItemIdx, strStoreItem + ".dayofweek", MyCaffe.basecode.descriptors.ValueStreamDescriptor.STREAM_VALUE_TYPE.NUMERIC, nOrdering++, dtStart, dtEnd, nSecondsPerStep);
                    int nStreamID_dayofmonth = db.AddKnownValueStream(nSrcID, nItemIdx, strStoreItem + ".dayofmonth", MyCaffe.basecode.descriptors.ValueStreamDescriptor.STREAM_VALUE_TYPE.NUMERIC, nOrdering++, dtStart, dtEnd, nSecondsPerStep);
                    int nStreamID_month = db.AddKnownValueStream(nSrcID, nItemIdx, strStoreItem + ".month", MyCaffe.basecode.descriptors.ValueStreamDescriptor.STREAM_VALUE_TYPE.NUMERIC, nOrdering++, dtStart, dtEnd, nSecondsPerStep);
                    int nStreamID_holidaytype = db.AddKnownValueStream(nSrcID, nItemIdx, strStoreItem + ".holidaytype", MyCaffe.basecode.descriptors.ValueStreamDescriptor.STREAM_VALUE_TYPE.CATEGORICAL, nOrdering++, dtStart, dtEnd, nSecondsPerStep);
                    int nStreamID_holidaylocaleid = db.AddKnownValueStream(nSrcID, nItemIdx, strStoreItem + ".holidaylocaleid", MyCaffe.basecode.descriptors.ValueStreamDescriptor.STREAM_VALUE_TYPE.CATEGORICAL, nOrdering++, dtStart, dtEnd, nSecondsPerStep);

                    int nStreamID_itemnum = db.AddStaticValueStream(nSrcID, nItemIdx, strStoreItem + ".itemnum", MyCaffe.basecode.descriptors.ValueStreamDescriptor.STREAM_VALUE_TYPE.CATEGORICAL, nOrdering++);
                    int nStreamID_storenum = db.AddStaticValueStream(nSrcID, nItemIdx, strStoreItem + ".storenum", MyCaffe.basecode.descriptors.ValueStreamDescriptor.STREAM_VALUE_TYPE.CATEGORICAL, nOrdering++);
                    int nStreamID_storecityid = db.AddStaticValueStream(nSrcID, nItemIdx, strStoreItem + ".storecityid", MyCaffe.basecode.descriptors.ValueStreamDescriptor.STREAM_VALUE_TYPE.CATEGORICAL, nOrdering++);
                    int nStreamID_storestateid = db.AddStaticValueStream(nSrcID, nItemIdx, strStoreItem + ".storestateid", MyCaffe.basecode.descriptors.ValueStreamDescriptor.STREAM_VALUE_TYPE.CATEGORICAL, nOrdering++);
                    int nStreamID_storetypeid = db.AddStaticValueStream(nSrcID, nItemIdx, strStoreItem + ".storetypeid", MyCaffe.basecode.descriptors.ValueStreamDescriptor.STREAM_VALUE_TYPE.CATEGORICAL, nOrdering++);
                    int nStreamID_storecluster = db.AddStaticValueStream(nSrcID, nItemIdx, strStoreItem + ".storecluster", MyCaffe.basecode.descriptors.ValueStreamDescriptor.STREAM_VALUE_TYPE.CATEGORICAL, nOrdering++);
                    int nStreamID_familyid = db.AddStaticValueStream(nSrcID, nItemIdx, strStoreItem + ".familyid", MyCaffe.basecode.descriptors.ValueStreamDescriptor.STREAM_VALUE_TYPE.CATEGORICAL, nOrdering++);
                    int nStreamID_classid = db.AddStaticValueStream(nSrcID, nItemIdx, strStoreItem + ".classid", MyCaffe.basecode.descriptors.ValueStreamDescriptor.STREAM_VALUE_TYPE.CATEGORICAL, nOrdering++);
                    int nStreamID_perishable = db.AddStaticValueStream(nSrcID, nItemIdx, strStoreItem + ".perishable", MyCaffe.basecode.descriptors.ValueStreamDescriptor.STREAM_VALUE_TYPE.CATEGORICAL, nOrdering++);

                    db.PutRawValue(nSrcID, nItemIdx, nStreamID_itemnum, nItemID);
                    db.PutRawValue(nSrcID, nItemIdx, nStreamID_storenum, store.StoreNum);
                    db.PutRawValue(nSrcID, nItemIdx, nStreamID_storecityid, store.CityID);
                    db.PutRawValue(nSrcID, nItemIdx, nStreamID_storestateid, store.StateID);
                    db.PutRawValue(nSrcID, nItemIdx, nStreamID_storetypeid, store.TypeID);
                    db.PutRawValue(nSrcID, nItemIdx, nStreamID_storecluster, store.Cluster);
                    db.PutRawValue(nSrcID, nItemIdx, nStreamID_familyid, item.FamilyID);
                    db.PutRawValue(nSrcID, nItemIdx, nStreamID_classid, item.ClassID);
                    db.PutRawValue(nSrcID, nItemIdx, nStreamID_perishable, item.Perishable);

                    for (int i=0; i<rgItems.Count; i++)
                    {
                        db.PutRawValue(nSrcID, nItemIdx, nStreamID_logsales, rgItems[i].LogSales);
                        db.PutRawValue(nSrcID, nItemIdx, nStreamID_oilprice, rgItems[i].OilPrice);
                        db.PutRawValue(nSrcID, nItemIdx, nStreamID_storetrx, rgItems[i].StoreTransactions);

                        db.PutRawValue(nSrcID, nItemIdx, nStreamID_dayofweek, (int)rgItems[i].Date.DayOfWeek);
                        db.PutRawValue(nSrcID, nItemIdx, nStreamID_dayofmonth, rgItems[i].Date.Day);
                        db.PutRawValue(nSrcID, nItemIdx, nStreamID_month, rgItems[i].Date.Month);
                        db.PutRawValue(nSrcID, nItemIdx, nStreamID_onpromotion, rgItems[i].OnPromotion);
                        db.PutRawValue(nSrcID, nItemIdx, nStreamID_holidaytype, rgItems[i].HolidayType);
                        db.PutRawValue(nSrcID, nItemIdx, nStreamID_holidaylocaleid, rgItems[i].HolidayLocaleID);
                    }

                    if (nItemIdx % 1000 == 0)
                        db.SaveRawValues();

                    if (sw.Elapsed.TotalMilliseconds > 1000)
                    {
                        sw.Restart();

                        if (m_evtCancel.WaitOne(0))
                            break;

                        double dfPct = (double)nItemIdx / (double)nItemCount;
                        m_log.WriteLine("Store_Item = " + strStoreItem + " - Saving '" + strSub + "' data to sql at " + dfPct.ToString("P") + " complete.");
                    }

                    db.SaveRawValues();
                    db.UpdateStreamCounts(nItemIdx, nStreamID_logsales, nStreamID_oilprice, nStreamID_storetrx, nStreamID_onpromotion, nStreamID_onpromotion, nStreamID_dayofweek, nStreamID_dayofmonth, nStreamID_month, nStreamID_itemnum, nStreamID_storenum, nStreamID_storecityid, nStreamID_storetypeid, nStreamID_storecluster, nStreamID_familyid, nStreamID_classid, nStreamID_perishable);
                    nItemIdx++;
                }
            }

            db.Close();

            return nSrcID;
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
        Dictionary<int, Dictionary<int, List<DataRecord>>> m_rgRecordsByStoreItem = new Dictionary<int, Dictionary<int, List<DataRecord>>>();
        List<Tuple<int, int, DateTime>> m_rgIndex = new List<Tuple<int, int, DateTime>>();
        int m_nIdx = 0;
        int m_nCount;
        Log m_log;
        CancelEvent m_evtCancel;
        Stopwatch m_sw = new Stopwatch();

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

        public Dictionary<int, Dictionary<int, List<DataRecord>>> RecordsByStoreItem
        {
            get { return m_rgRecordsByStoreItem; }
        }

        public void AddRange(List<DataRecord> rg)
        {
            foreach (DataRecord rec in rg)
            {
                Add(rec);
            }
        }

        public void Add(DataRecord item)
        {
            if (!item.IsValid)
                return;

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

            if (!m_rgRecordsByStoreItem.ContainsKey(item.StoreNum))
                m_rgRecordsByStoreItem.Add(item.StoreNum, new Dictionary<int, List<DataRecord>>());

            if (!m_rgRecordsByStoreItem[item.StoreNum].ContainsKey(item.ItemNum))
                m_rgRecordsByStoreItem[item.StoreNum].Add(item.ItemNum, new List<DataRecord>() { item });
            else
                m_rgRecordsByStoreItem[item.StoreNum][item.ItemNum].Add(item);

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

        public DataRecordCollection Split(string strName, double dfPctStart, double dfPctEnd)
        {
            DataRecordCollection col = new DataRecordCollection(m_log, m_evtCancel);
            List<KeyValuePair<DateTime, List<DataRecord>>> rgRecords = m_rgRecordsByDate.OrderBy(p => p.Key).ToList();
            int nStart = (int)(rgRecords.Count * dfPctStart);
            int nEnd = (int)(rgRecords.Count * dfPctEnd);

            m_sw.Restart();
            for (int i=nStart; i<nEnd; i++)
            {
                col.AddRange(rgRecords[i].Value);

                if (m_sw.Elapsed.TotalMilliseconds > 1000)
                {
                    double dfPct = (i - nStart) / (double)(nEnd - nStart);
                    m_log.WriteLine("Splitting '" + strName + "' at " + dfPct.ToString("P") + "...");
                    m_sw.Restart();
                }
            }

            return col;
        }

        public void CalculateStatistics(DataRecord.FIELD field, List<DataRecord> col, out double dfMean1, out double dfStdev1, bool bOnlyNonZero = false)
        {
            double dfTotal = col.Sum(p => p.Item(field));
            int nCount = col.Count;
            if (bOnlyNonZero)
                nCount = col.Count(p => p.Item(field) != 0);

            double dfMean = dfTotal / nCount;
            double dfStdev = Math.Sqrt(col.Sum(p => Math.Pow(p.Item(field) - dfMean, 2)) / nCount);
            dfMean1 = dfMean;
            dfStdev1 = dfStdev;
        }

        public void Normalize(DataRecord.FIELD dt, List<DataRecord> col, double dfMean, double dfStdev)
        {
            for (int i = 0; i < col.Count; i++)
            {
                double dfVal = col[i].Item(dt);
                dfVal = (dfVal - dfMean) / dfStdev;
                col[i].NormalizedItem(dt, dfVal);
            }
        }

        private void normalize(DataRecord.FIELD field, List<DataRecord> col, Dictionary<DataRecord.FIELD, Tuple<double, double>> rgScalers)
        {
            double dfMean;
            double dfStdev;

            if (rgScalers.ContainsKey(field))
            {
                dfMean = rgScalers[field].Item1;
                dfStdev = rgScalers[field].Item2;
            }
            else
            {
                CalculateStatistics(field, col, out dfMean, out dfStdev, true);
                rgScalers.Add(field, new Tuple<double, double>(dfMean, dfStdev));
            }

            Normalize(field, col, dfMean, dfStdev);
        }

        public bool Normalize(Stopwatch sw, Log log, CancelEvent evtCancel, Dictionary<int, Dictionary<int, Dictionary<DataRecord.FIELD, Tuple<double, double>>>> rgScalers)
        {
            int nTotal = m_rgRecordsByStoreItem.Sum(p => p.Value.Count);
            int nIdx = 0;
            sw.Restart();

            foreach (KeyValuePair<int, Dictionary<int, List<DataRecord>>> kv in m_rgRecordsByStoreItem)
            {
                if (!rgScalers.ContainsKey(kv.Key))
                    rgScalers.Add(kv.Key, new Dictionary<int, Dictionary<DataRecord.FIELD, Tuple<double, double>>>());

                foreach (KeyValuePair<int, List<DataRecord>> kv1 in kv.Value)
                {
                    Dictionary<DataRecord.FIELD, Tuple<double, double>> rgScalers1;

                    if (!rgScalers[kv.Key].ContainsKey(kv1.Key))
                        rgScalers[kv.Key].Add(kv1.Key, new Dictionary<DataRecord.FIELD, Tuple<double, double>>());

                    rgScalers1 = rgScalers[kv.Key][kv1.Key];

                    normalize(DataRecord.FIELD.LOG_UNIT_SALES, kv1.Value, rgScalers1);
                    normalize(DataRecord.FIELD.OIL_PRICE, kv1.Value, rgScalers1);
                    normalize(DataRecord.FIELD.STORE_TRANSACTIONS, kv1.Value, rgScalers1);
                    normalize(DataRecord.FIELD.DAY_OF_WEEK, kv1.Value, rgScalers1);
                    normalize(DataRecord.FIELD.DAY_OF_MONTH, kv1.Value, rgScalers1);
                    normalize(DataRecord.FIELD.MONTH, kv1.Value, rgScalers1);

                    if (sw.Elapsed.TotalMilliseconds > 1000)
                    {
                        sw.Restart();

                        double dfPct = (double)nIdx / nTotal;
                        log.WriteLine("Normalizing data at " + dfPct.ToString("P") + " complete.");

                        if (evtCancel.WaitOne(0))
                            return false;
                    }

                    nIdx++;
                }
            }

            return true;
        }
    }

    public class DataRecord
    {
        int m_nID;
        DateTime m_dt;                  // 0 hist/fut num (unix time)
        double[] m_rgFields = new double[19];
        double[] m_rgFieldsNormalized = new double[19];
        bool m_bValid = true;

        public enum FIELD
        {
            UNIT_SALES = 0,
            LOG_UNIT_SALES = 1,         // 0 hist num
            OIL_PRICE = 2,              // 1 hist num
            STORE_TRANSACTIONS = 3,     // 2 hist num
            DAY_OF_WEEK = 4,            // 3 hist/fut num
            DAY_OF_MONTH = 5,           // 4 hist/fut num
            MONTH = 6,                  // 5 hist/fut num
            STORE_NUM = 7,              // static cat 0
            STORE_CITY_ID = 8,          // static cat 1
            STORE_STATE_ID = 9,         // static cat 2
            STORE_TYPE_ID = 10,          // static cat 3
            STORE_CLUSTER = 11,         // static cat 4
            ITEM_NUM = 12,              // static cat 5
            ITEM_FAMILY_ID = 13,        // static cat 6
            ITEM_CLASS_ID = 14,         // static cat 7
            ITEM_PERISHABLE = 15,       // static cat 8
            ON_PROMOTION = 16,          // static cat 9
            HOLIDAY_TYPE = 17,          // 0 hist/fut cat
            HOLIDAY_LOCALE_ID = 18      // 1 hist/fut cat
        }

        public DataRecord(DateTime dt, string[] rgstr)
        {
            m_nID = int.Parse(rgstr[0]);
            m_dt = dt;
            int nStoreNum = int.Parse(rgstr[2]);
            int nItemNum = int.Parse(rgstr[3]);
            float fUnitSales = float.Parse(rgstr[4]);
            float fLogSales = (float)Math.Log(fUnitSales);

            if (double.IsNaN(fLogSales) || double.IsInfinity(fLogSales))
            {
                m_bValid = false;
                fLogSales = 0;
            }

            int nOnPromotion = 0;
            if (rgstr[5].ToLower() == "true")
                nOnPromotion = 1;

            m_rgFields[(int)FIELD.UNIT_SALES] = fUnitSales;
            m_rgFields[(int)FIELD.LOG_UNIT_SALES] = fLogSales;
            m_rgFields[(int)FIELD.ON_PROMOTION] = nOnPromotion;
            m_rgFields[(int)FIELD.STORE_NUM] = nStoreNum;
            m_rgFields[(int)FIELD.ITEM_NUM] = nItemNum;
            m_rgFields[(int)FIELD.DAY_OF_WEEK] = (int)dt.DayOfWeek;
            m_rgFields[(int)FIELD.DAY_OF_MONTH] = dt.Day;
            m_rgFields[(int)FIELD.MONTH] = dt.Month;
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

        public bool IsValid
        {
            get { return m_bValid; }
        }   

        public double Item(FIELD field)
        {
            return m_rgFields[(int)field];
        }

        public void Item(FIELD field, double dfVal)
        {
            m_rgFields[(int)field] = dfVal;
        }

        public double NormalizedItem(FIELD field)
        {
            return m_rgFieldsNormalized[(int)field];
        }

        public void NormalizedItem(FIELD field, double dfVal)
        {
            m_rgFieldsNormalized[(int)field] = dfVal;
        }

        public static int NumStaticCategorical
        {
            get { return 9;  }
        }

        public static int NumStaticNumeric
        {
            get { return 0; }
        }

        public static int NumHistoricalCategorical
        {
            get { return 3; }
        }

        public static int NumHistoricalNumeric
        {
            get { return 6; }
        }

        public static int NumFutureCategorical
        {
            get { return 3; }
        }

        public static int NumFutureNumeric
        {
            get { return 0; }
        }

        public string ToString(int nIdx, Dictionary<int, StoreRecord> rgStores, Dictionary<int, ItemRecord> rgItems, Dictionary<DateTime, HolidayRecord> rgHolidaysLocal, Dictionary<DateTime, HolidayRecord> rgHolidaysRegional, Dictionary<DateTime, HolidayRecord> rgHolidaysNational)
        {
            StringBuilder sb = new StringBuilder();
            int nStoreNum = (int)m_rgFields[(int)FIELD.STORE_NUM];
            int nItemNum = (int)m_rgFields[(int)FIELD.ITEM_NUM];
            string strStoreNum = nStoreNum.ToString();
            string strItemNum =  nItemNum.ToString();
            string strTrajID = strStoreNum + "_" + strItemNum;
            string strDate = m_dt.ToString("yyyy-MM-dd");
            string strUniqueID = strTrajID + "_" + strDate;
            StoreRecord store = rgStores[nStoreNum];
            ItemRecord item = rgItems[nItemNum];
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
            sb.Append(m_rgFields[(int)FIELD.UNIT_SALES].ToString("N0"));
            sb.Append(',');
            sb.Append(m_rgFields[(int)FIELD.ON_PROMOTION] == 0 ? "FALSE" : "TRUE"); 
            sb.Append(',');
            sb.Append(strTrajID);
            sb.Append(',');
            sb.Append(strUniqueID);
            sb.Append(',');
            sb.Append("1,");
            sb.Append(strDate);
            sb.Append(',');
            sb.Append(m_rgFields[(int)FIELD.LOG_UNIT_SALES].ToString());
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
            sb.Append(m_rgFields[(int)FIELD.STORE_TRANSACTIONS].ToString());
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
            if (StoreNum != store.StoreNum)
                throw new Exception("Store number mismatch.");

            StoreCityID = store.CityID;
            StoreStateID = store.StateID;
            StoreTypeID = store.TypeID;
            StoreCluster = store.Cluster;
        }

        public void AddItemInfo(ItemRecord item)
        {
            if (ItemNum != item.ItemNum)
                throw new Exception("Item number mismatch.");

            ItemFamilyID = item.FamilyID;
            ItemClassID = item.ClassID;
            ItemPerishable = item.Perishable;
        }

        public void AddHolidayInfo(HolidayRecord holiday)
        {
            HolidayType = 0;
            HolidayLocaleID = 0;

            if (m_dt == holiday.Date)
            { 
                HolidayType = holiday.HolidayType;
                HolidayLocaleID = holiday.HolidayLocaleID;
            }
        }

        public void AddTransactionInfo(TransactionRecord trx)
        {
            if (StoreNum == trx.StoreNum)
                StoreTransactions = trx.TransactionCount;
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
            get { return (int)m_rgFields[(int)FIELD.STORE_NUM]; }
        }

        public int StoreCityID
        {
            get { return (int)m_rgFields[(int)FIELD.STORE_CITY_ID]; }
            set { m_rgFields[(int)FIELD.STORE_CITY_ID] = value; }
        }

        public int StoreStateID
        {
            get { return (int)m_rgFields[(int)FIELD.STORE_STATE_ID]; }
            set { m_rgFields[(int)FIELD.STORE_STATE_ID] = value; }
        }

        public int StoreTypeID
        {
            get { return (int)m_rgFields[(int)FIELD.STORE_TYPE_ID]; }
            set { m_rgFields[(int)FIELD.STORE_TYPE_ID] = value; }
        }

        public int StoreCluster
        {
            get { return (int)m_rgFields[(int)FIELD.STORE_CLUSTER]; }
            set { m_rgFields[(int)FIELD.STORE_CLUSTER] = value; }
        }

        public int StoreTransactions
        {
            get { return (int)m_rgFields[(int)FIELD.STORE_TRANSACTIONS]; }
            set { m_rgFields[(int)FIELD.STORE_TRANSACTIONS] = value; }
        }

        public int ItemNum
        {
            get { return (int)m_rgFields[(int)FIELD.ITEM_NUM]; }
            set { m_rgFields[(int)FIELD.ITEM_NUM] = value; }
        }

        public int ItemFamilyID
        {
            get { return (int)m_rgFields[(int)FIELD.ITEM_FAMILY_ID]; }
            set { m_rgFields[(int)FIELD.ITEM_FAMILY_ID] = value; }
        }

        public int ItemClassID
        {
            get { return (int)m_rgFields[(int)FIELD.ITEM_CLASS_ID]; }
            set { m_rgFields[(int)FIELD.ITEM_CLASS_ID] = value; }
        }

        public int ItemPerishable
        {
            get { return (int)m_rgFields[(int)FIELD.ITEM_PERISHABLE]; }
            set { m_rgFields[(int)FIELD.ITEM_PERISHABLE] = value; }
        }

        public float UnitSales
        {
            get { return (float)m_rgFields[(int)FIELD.UNIT_SALES]; }
        }

        public float LogSales
        {
            get { return (float)m_rgFields[(int)FIELD.LOG_UNIT_SALES]; }
        }

        public int OnPromotion
        {
            get { return (int)m_rgFields[(int)FIELD.ON_PROMOTION]; }
        }

        public float OilPrice
        {
            get { return (float)m_rgFields[(int)FIELD.OIL_PRICE]; }
            set { m_rgFields[(int)FIELD.OIL_PRICE] = value; }
        }

        public int HolidayType
        {
            get { return (int)m_rgFields[(int)FIELD.HOLIDAY_TYPE]; }
            set { m_rgFields[(int)FIELD.HOLIDAY_TYPE] = value; }
        }

        public int HolidayLocaleID
        {
            get { return (int)m_rgFields[(int)FIELD.HOLIDAY_LOCALE_ID]; }
            set { m_rgFields[(int)FIELD.HOLIDAY_LOCALE_ID] = value; }
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
