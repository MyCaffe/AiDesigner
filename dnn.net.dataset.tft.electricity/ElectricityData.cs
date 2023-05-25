﻿using MyCaffe.basecode;
using MyCaffe.common;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Runtime.InteropServices;
using System.Runtime.Remoting.Messaging;
using System.Text;
using System.Threading.Tasks;
using System.Xml;

namespace DNN.net.dataset.tft.electricity
{
    public class ElectricityData
    {
        string m_strDataFile;
        DataTable m_data = new DataTable();
        Dictionary<int, string> m_rgCustomers = new Dictionary<int, string>();
        Log m_log;
        CancelEvent m_evtCancel;
        Stopwatch m_sw = new Stopwatch();


        public ElectricityData(string strDataFile, Log log, CancelEvent evtCancel)
        {
            m_strDataFile = strDataFile;
            m_log = log;
            m_evtCancel = evtCancel;
        }

        public ElectricityData SplitData(double dfPctStart, double dfPctEnd)
        {
            ElectricityData data = new ElectricityData(null, m_log, m_evtCancel);

            data.m_rgCustomers = m_rgCustomers;
            data.m_data = m_data.Split(dfPctStart, dfPctEnd);

            return data;
        }

        public bool LoadData(DateTime dtStart, DateTime dtEnd)
        {
            m_data.StartTime = DateTime.MaxValue;
            m_data.Clear();
            m_rgCustomers.Clear();

            FileInfo fi = new FileInfo(m_strDataFile);
            long lTotalLen = fi.Length;
            long lIdx = 0;

            m_sw.Restart();

            using (StreamReader sr = new StreamReader(m_strDataFile))
            {
                string strLine = sr.ReadLine();
                lIdx += strLine.Length;

                string[] rgstrCustomers = strLine.Split(';');
                for (int i = 1; i < rgstrCustomers.Length; i++)
                {
                    string strID = rgstrCustomers[i].Trim('"', ' ');
                    if (!string.IsNullOrEmpty(strID))
                        m_rgCustomers.Add(m_rgCustomers.Count + 1, strID);
                }

                strLine = sr.ReadLine();
                while (!string.IsNullOrEmpty(strLine))
                {
                    lIdx += strLine.Length;

                    string[] rgstrData = strLine.Split(';');
                    DateTime dt = DateTime.Parse(rgstrData[0].Trim('"'));

                    if (dt >= dtStart && dt < dtEnd)
                    {
                        if (dt < m_data.StartTime)
                            m_data.StartTime = dt;

                        for (int i = 1; i < rgstrData.Length; i++)
                        {
                            string strVal = rgstrData[i].Trim('"');
                            strVal = strVal.Replace(',', '.');

                            double dfVal = double.Parse(strVal);
                            if (dfVal >= 0)
                                m_data.Add(new DataRecord(i, dt, dfVal));
                            else
                                m_data.Add(new DataRecord(i, dt, 0));
                        }
                    }

                    strLine = sr.ReadLine();

                    if (m_sw.Elapsed.TotalMilliseconds > 1000)
                    {
                        m_sw.Restart();

                        if (m_evtCancel.WaitOne(0))
                            return false;

                        double dfPct = (double)lIdx / (double)lTotalLen;
                        m_log.WriteLine("Loading data file at " + dfPct.ToString("P") + " complete.");
                    }
                }
            }

            if (!NormalizeData())
                return false;

            return true;
        }

        public bool NormalizeData()
        {
            m_sw.Restart();

            DataTable dt = m_data.ResampleByHour(m_sw, m_log, m_evtCancel);
            if (dt == null)
                return false;

            m_data = dt;

            return m_data.Normalize(m_sw, m_log, m_evtCancel);
        }

        public bool SaveAsNumpy(string strPath, string strSub)
        {
            string strPath1 = strPath + "\\preprocessed";

            if (!Directory.Exists(strPath1))
                Directory.CreateDirectory(strPath1);

            m_log.WriteLine("Saving sync data to numpy files...");
            if (!saveSyncFile(strPath1, strSub))
                return false;

            m_log.WriteLine("Saving observed numeric data to numpy files...");
            if (!saveObservedNumericFile(strPath1, strSub))
                return false;

            m_log.WriteLine("Saving known numeric data to numpy files...");
            if (!saveKnownNumericFile(strPath1, strSub))
                return false;

            m_log.WriteLine("Saving static categorical data to numpy files...");
            if (!saveStaticCategoricalFile(strPath1, strSub))
                return false;

            m_log.WriteLine("Saving schema file...");
            if (!saveSchemaFile(strPath1, strSub, m_data.Columns))
                return false;

            return false;
        }

        private bool saveSyncFile(string strPath, string strSub)
        {
            int nCustomers = m_rgCustomers.Count;
            int nRecords = m_data.RecordsPerCustomer;
            int nFields = 2;
            long[] rgData = new long[nCustomers * nRecords * nFields];
            long[] rgCustomer = new long[nRecords * nFields];
            int nIdxCustomer = 0;

            foreach (KeyValuePair<int, DataRecordCollection> kv in m_data.RecordsByCustomer)
            {
                int nIdx = 0;

                foreach (DataRecord rec in kv.Value.Items)
                {
                    rgCustomer[nIdx++] = ((DateTimeOffset)rec.Date).ToUnixTimeSeconds();
                    rgCustomer[nIdx++] = rec.CustomerID;
                }

                Array.Copy(rgCustomer, 0, rgData, rgCustomer.Length * nIdxCustomer, rgCustomer.Length);
                nIdxCustomer++;
            }

            Blob<float>.SaveToNumpy(strPath + "\\" + strSub + "_sync.npy", rgData, new int[] { nCustomers, nRecords, nFields });
            return true;
        }

        private bool saveObservedNumericFile(string strPath, string strSub)
        {
            int nCustomers = m_rgCustomers.Count;
            int nRecords = m_data.RecordsPerCustomer;
            int nFields = 1;
            float[] rgData = new float[nCustomers * nRecords * nFields];
            float[] rgCustomer = new float[nRecords * nFields];
            int nIdxCustomer = 0;

            foreach (KeyValuePair<int, DataRecordCollection> kv in m_data.RecordsByCustomer)
            {
                int nIdx = 0;

                foreach (DataRecord rec in kv.Value.Items)
                {
                    rgCustomer[nIdx++] = (float)rec.NormalizedPowerUsage;
                }

                Array.Copy(rgCustomer, 0, rgData, rgCustomer.Length * nIdxCustomer, rgCustomer.Length);
                nIdxCustomer++;
            }

            Blob<float>.SaveToNumpy(strPath + "\\" + strSub + "_observed_num.npy", rgData, new int[] { nCustomers, nRecords, nFields });
            return true;
        }

        private bool saveKnownNumericFile(string strPath, string strSub)
        {
            int nCustomers = m_rgCustomers.Count;
            int nRecords = m_data.RecordsPerCustomer;
            int nFields = 2;
            float[] rgData = new float[nCustomers * nRecords * nFields];
            float[] rgCustomer = new float[nRecords * nFields];
            int nIdxCustomer = 0;

            foreach (KeyValuePair<int, DataRecordCollection> kv in m_data.RecordsByCustomer)
            {
                int nIdx = 0;

                foreach (DataRecord rec in kv.Value.Items)
                {
                    rgCustomer[nIdx++] = (float)rec.NormalizedHour;
                    rgCustomer[nIdx++] = (float)rec.NormalizedHourFromStart;
                }

                Array.Copy(rgCustomer, 0, rgData, rgCustomer.Length * nIdxCustomer, rgCustomer.Length);
                nIdxCustomer++;
            }

            Blob<float>.SaveToNumpy(strPath + "\\" + strSub + "_known_num.npy", rgData, new int[] { nCustomers, nRecords, nFields });
            return true;
        }

        private bool saveStaticCategoricalFile(string strPath, string strSub)
        {
            int nCustomers = m_rgCustomers.Count;
            int nFields = 1;
            long[] rgData = new long[nCustomers * nFields];
            int nIdxCustomer = 0;

            foreach (KeyValuePair<int, DataRecordCollection> kv in m_data.RecordsByCustomer)
            {
                rgData[nIdxCustomer] = kv.Value[0].CustomerID;
                nIdxCustomer++;
            }

            Blob<float>.SaveToNumpy(strPath + "\\" + strSub + "_static_cat.npy", rgData, new int[] { nCustomers, nFields });
            return true;
        }

        private void saveSync(XmlTextWriter tw)
        {
            tw.WriteStartElement("Sync");
                tw.WriteElementString("File", "sync.npy");

                tw.WriteStartElement("Field");
                    tw.WriteAttributeString("Index", "0");
                    tw.WriteAttributeString("DataType", "REAL");
                    tw.WriteAttributeString("InputType", "TIME");
                    tw.WriteValue("time");
                tw.WriteEndElement();

                tw.WriteStartElement("Field");
                    tw.WriteAttributeString("Index", "1");
                    tw.WriteAttributeString("DataType", "REAL");
                    tw.WriteAttributeString("InputType", "ID");
                    tw.WriteValue("customer id");
                tw.WriteEndElement();
            tw.WriteEndElement();
        }

        private void saveObserved(XmlTextWriter tw)
        {
            tw.WriteStartElement("Observed");
                tw.WriteStartElement("Numeric");
                    tw.WriteElementString("File", "observed_num.npy");
                    
                    tw.WriteStartElement("Field");
                        tw.WriteAttributeString("Index", "0");
                        tw.WriteAttributeString("DataType", "REAL");
                        tw.WriteAttributeString("InputType", "OBSERVED,TARGET");
                        tw.WriteValue("log power usage");
                    tw.WriteEndElement();
                tw.WriteEndElement();

                tw.WriteStartElement("Categorical");
                tw.WriteEndElement();
            tw.WriteEndElement();
        }

        private void saveKnown(XmlTextWriter tw)
        {
            tw.WriteStartElement("Known");
                tw.WriteStartElement("Numeric");
                    tw.WriteElementString("File", "known_num.npy");
                    
                    tw.WriteStartElement("Field");
                        tw.WriteAttributeString("Index", "0");
                        tw.WriteAttributeString("DataType", "REAL");
                        tw.WriteAttributeString("InputType", "KNOWN");
                        tw.WriteAttributeString("DerivedFrom", "TIME:0");
                        tw.WriteAttributeString("Calculation", "HR");
                        tw.WriteValue("hour");
                    tw.WriteEndElement();

                    tw.WriteStartElement("Field");
                        tw.WriteAttributeString("Index", "1");
                        tw.WriteAttributeString("DataType", "REAL");
                        tw.WriteAttributeString("InputType", "KNOWN");
                        tw.WriteAttributeString("DerivedFrom", "TIME:0");
                        tw.WriteAttributeString("Calculation", "HRFS");
                        tw.WriteValue("hour from start");
                    tw.WriteEndElement();
                tw.WriteEndElement();

                tw.WriteStartElement("Categorical");
                tw.WriteEndElement();
            tw.WriteEndElement();
        }

        private void saveStatic(XmlTextWriter tw)
        {
            tw.WriteStartElement("Static");
                tw.WriteStartElement("Numeric");
                tw.WriteEndElement();

                tw.WriteStartElement("Categorical");
                    tw.WriteElementString("File", "static_cat.npy");

                    tw.WriteStartElement("Field");
                        tw.WriteAttributeString("Index", "0");
                        tw.WriteAttributeString("DataType", "REAL");
                        tw.WriteAttributeString("InputType", "STATIC");
                        tw.WriteValue("customer id");
                    tw.WriteEndElement();
                tw.WriteEndElement();
            tw.WriteEndElement();
        }

        private void saveLoookups(XmlTextWriter tw)
        {
            tw.WriteStartElement("Lookup");
                tw.WriteAttributeString("Name", "customer id");
                tw.WriteAttributeString("Type", "Categorical");

                foreach (KeyValuePair<int, string> kv in m_rgCustomers)
                {
                    tw.WriteStartElement("Item");
                        tw.WriteAttributeString("Index", kv.Key.ToString());
                        tw.WriteAttributeString("ValidRangeStartIdx", m_data.ValidRangeByCustomer[kv.Key].Item1.ToString());
                        tw.WriteAttributeString("ValidRangeEndIdx", m_data.ValidRangeByCustomer[kv.Key].Item2.ToString());
                        tw.WriteValue(kv.Value);
                    tw.WriteEndElement();
                }

            tw.WriteEndElement();
        }

        private bool saveSchemaFile(string strPath, string strSub, int nColumns)
        {
            using (XmlTextWriter tw = new XmlTextWriter(strPath + "\\" + strSub + "_schema.xml", null))
            {
                tw.WriteStartDocument();
                    tw.WriteStartElement("Schema");
                        tw.WriteStartElement("Data");
                        tw.WriteAttributeString("Columns", nColumns.ToString());
                            saveSync(tw);
                            saveObserved(tw);
                            saveKnown(tw);
                            saveStatic(tw);
                        tw.WriteEndElement();

                        tw.WriteStartElement("Lookups");
                            saveLoookups(tw);
                        tw.WriteEndElement();
                    tw.WriteEndElement();
                tw.WriteEndDocument();
            }

            return true;
        }
    }

    public class DataRecordCollection
    {
        List<DataRecord> m_rgItems = new List<DataRecord>();

        public DataRecordCollection()
        {
        }

        public void Add(DataRecord rec)
        {
            m_rgItems.Add(rec);
        }

        public DataRecord this[int nIdx]
        {
            get { return m_rgItems[nIdx]; }
        }

        public int Count
        {
            get { return m_rgItems.Count; }
        }

        public void SetStart(DateTime dt)
        {
            foreach (DataRecord rec in m_rgItems)
            {
                rec.SetStart(dt);
            }
        }

        public void CalculateStatistics(DataRecord.FIELD field, out double dfMean1, out double dfStdev1, bool bOnlyNonZero = false)
        {
            double dfTotal = m_rgItems.Sum(p => p.Item(field));
            int nCount = m_rgItems.Count;
            if (bOnlyNonZero)
                nCount = m_rgItems.Count(p => p.Item(field) != 0);

            double dfMean = dfTotal / nCount;
            double dfStdev = Math.Sqrt(m_rgItems.Sum(p => Math.Pow(p.Item(field) - dfMean, 2)) / nCount);
            dfMean1 = dfMean;
            dfStdev1 = dfStdev;
        }

        public void Normalize(DataRecord.FIELD src, DataRecord.FIELD dst, double dfMean, double dfStdev)
        {
            for (int i = 0; i < m_rgItems.Count; i++)
            {
                if (m_rgItems[i].IsValid)
                {
                    double dfVal = m_rgItems[i].Item(src);
                    dfVal = (dfVal - dfMean) / dfStdev;
                    m_rgItems[i].Item(dst, dfVal);
                }
            }
        }

        public List<DataRecord> Items
        {
            get { return m_rgItems; }
        }
    }

    public class DataRecord
    {
        int m_nCustomerID;
        DateTime m_dt;
        double[] m_rgFields = new double[7];

        public enum FIELD
        {
            POWER_USAGE = 0,
            LOG_POWER_USAGE = 1,
            NORMALIZED_POWER_USAGE = 2,
            HOUR = 3,
            NORMALIZED_HOUR = 4,
            HOURS_FROM_START = 5,
            NORMALIZED_HOURS_FROM_START = 6
        }

        public DataRecord(int nCustomerID, DateTime dt, double dfPowerUsage)
        {
            m_nCustomerID = nCustomerID;
            m_dt = dt;
            m_rgFields[(int)FIELD.HOUR] = dt.Hour;
            m_rgFields[(int)FIELD.POWER_USAGE] = dfPowerUsage;

            double dfLogPowerUsage = (dfPowerUsage == 0) ? 0 : Math.Log(dfPowerUsage);
            if (double.IsNaN(dfLogPowerUsage) || double.IsInfinity(dfLogPowerUsage))
                dfLogPowerUsage = 0;

            m_rgFields[(int)FIELD.LOG_POWER_USAGE] = dfLogPowerUsage;
        }

        public bool IsValid
        {
            get { return (PowerUsage > 0) ? true : false; }
        }

        public int CustomerID
        {
            get { return m_nCustomerID; }
        }

        public DateTime Date
        {
            get { return m_dt; }
        }

        public double Item(FIELD field)
        {
            return m_rgFields[(int)field];
        }

        public void Item(FIELD field, double dfVal)
        {
            m_rgFields[(int)field] = dfVal;
        }

        public double PowerUsage
        {
            get { return m_rgFields[(int)FIELD.POWER_USAGE]; }
        }

        public double LogPowerUsage
        {
            get { return m_rgFields[(int)FIELD.LOG_POWER_USAGE]; }
        }

        public double NormalizedPowerUsage
        {
            get { return m_rgFields[(int)FIELD.NORMALIZED_POWER_USAGE]; }
            set { m_rgFields[(int)FIELD.NORMALIZED_POWER_USAGE] = value; }
        }

        public double Hour
        {
            get { return m_rgFields[(int)FIELD.HOUR]; }
        }

        public double NormalizedHour
        {
            get { return m_rgFields[(int)FIELD.NORMALIZED_HOUR]; }
            set { m_rgFields[(int)FIELD.NORMALIZED_HOUR] = value; }
        }

        public double HoursFromStart
        {
            get { return m_rgFields[(int)FIELD.HOURS_FROM_START]; }
            set { m_rgFields[(int)FIELD.HOURS_FROM_START] = value; }
        }

        public double NormalizedHourFromStart
        {
            get { return m_rgFields[(int)FIELD.NORMALIZED_HOURS_FROM_START]; }
            set { m_rgFields[(int)FIELD.NORMALIZED_HOURS_FROM_START] = value; }
        }

        public void SetStart(DateTime dt)
        {
            TimeSpan ts = m_dt - dt;
            m_rgFields[(int)FIELD.HOURS_FROM_START] = ts.TotalHours;
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append(m_dt.ToString());
            sb.Append(' ');
            sb.Append(IsValid ? "VALID" : "INVALID");
            sb.Append(' ');
            sb.Append("pwr = ");
            sb.Append(PowerUsage.ToString());
            sb.Append(" logpwr = ");
            sb.Append(LogPowerUsage.ToString());
            sb.Append(" normpwr = ");
            sb.Append(NormalizedPowerUsage.ToString());

            return sb.ToString();
        }
    }

    public class DataTable
    {
        int m_nCount = 0;
        Dictionary<int, DataRecordCollection> m_rgRecordsByCustomer = new Dictionary<int, DataRecordCollection>();
        Dictionary<int, Tuple<int, int>> m_rgValidRangeByCustomer = new Dictionary<int, Tuple<int, int>>();
        DateTime m_dtStart = DateTime.MinValue;

        public DataTable()
        {
        }

        public DataTable Split(double dfPctStart, double dfPctEnd)
        {
            DataTable dt = new DataTable();
            foreach (KeyValuePair<int, DataRecordCollection> kv in m_rgRecordsByCustomer)
            {
                int nStartIdx = (int)(kv.Value.Count * dfPctStart);
                int nEndIdx = (int)(kv.Value.Count * dfPctEnd);
                int nValidStartIdx = -1;
                int nValidEndIDx = -1;

                for (int i = nStartIdx; i < nEndIdx; i++)
                {
                    DataRecord rec = kv.Value[i];

                    if (rec.IsValid)
                    {
                        if (nValidStartIdx < 0)
                            nValidStartIdx = i - nStartIdx;
                        nValidEndIDx = i - nStartIdx;
                    }

                    dt.Add(rec);
                }

                dt.m_rgValidRangeByCustomer.Remove(kv.Key);
                dt.m_rgValidRangeByCustomer.Add(kv.Key, new Tuple<int, int>(nValidStartIdx, nValidEndIDx));
            }

            return dt;
        }

        public void Add(DataRecord rec)
        {
            if (!m_rgRecordsByCustomer.ContainsKey(rec.CustomerID))
                m_rgRecordsByCustomer.Add(rec.CustomerID, new DataRecordCollection());

            m_rgRecordsByCustomer[rec.CustomerID].Add(rec);

            m_nCount++;
        }

        public int Columns
        {
            get { return m_rgRecordsByCustomer.First().Value.Count; }
        }

        public int Count
        {
            get { return m_nCount; }
        }

        public int RecordsPerCustomer
        {
            get { return m_rgRecordsByCustomer.First().Value.Count; }
        }

        public long[] TimeSync
        {
            get { return m_rgRecordsByCustomer.First().Value.Items.Select(p => ((DateTimeOffset)p.Date).ToUnixTimeSeconds()).ToArray(); }
        }

        public DateTime StartTime
        {
            get { return m_dtStart; }
            set { m_dtStart = value; }
        }

        public Dictionary<int, DataRecordCollection> RecordsByCustomer
        {
            get { return m_rgRecordsByCustomer; }
        }

        public Dictionary<int, Tuple<int, int>> ValidRangeByCustomer
        {
            get { return m_rgValidRangeByCustomer; }
        }

        public void Clear()
        {
            m_rgRecordsByCustomer.Clear();
        }

        public DataTable ResampleByHour(Stopwatch sw, Log log, CancelEvent evtCancel)
        {
            DateTime dtEarliestStart = DateTime.MaxValue;
            DataTable dt = new DataTable();

            sw.Restart();

            int nCustomerIdx = 0;
            foreach (KeyValuePair<int, DataRecordCollection> kv in m_rgRecordsByCustomer)
            {
                int nStartIdx = -1;
                int nEndIdx = 0;
                List<DataRecord> rgRecs = kv.Value.Items.OrderBy(p => p.Date).ToList();

                DateTime dtStart = rgRecs[0].Date;
                dtStart = new DateTime(dtStart.Year, dtStart.Month, dtStart.Day, dtStart.Hour, 0, 0);
                DateTime dtEnd = dtStart + TimeSpan.FromHours(1);

                if (dtStart < dtEarliestStart)
                    dtEarliestStart = dtStart;

                int nIdx = 0;
                while (nIdx < rgRecs.Count)
                {
                    double dfItemTotal = 0;
                    int nItemCount = 0;
                    while (nIdx < rgRecs.Count && rgRecs[nIdx].Date < dtEnd)
                    {
                        if (rgRecs[nIdx].IsValid)
                        {
                            dfItemTotal += rgRecs[nIdx].PowerUsage;
                            nItemCount++;
                        }

                        nIdx++;
                    }

                    DataRecord rec = new DataRecord(kv.Key, dtStart, (nItemCount == 0) ? 0 : dfItemTotal / nItemCount);
                    dt.Add(rec);

                    if (rec.IsValid)
                    {
                        if (nStartIdx == -1)
                            nStartIdx = dt.m_rgRecordsByCustomer[kv.Key].Count - 1;

                        nEndIdx = dt.m_rgRecordsByCustomer[kv.Key].Count - 1;
                    }

                    dtStart = dtEnd;
                    dtEnd += TimeSpan.FromHours(1);

                    if (sw.Elapsed.TotalMilliseconds > 1000)
                    {
                        sw.Restart();

                        double dfPct = (double)nCustomerIdx/m_rgRecordsByCustomer.Count;
                        log.WriteLine("Resampling data at " + dfPct.ToString("P") + " complete.");

                        if (evtCancel.WaitOne(0))
                            return null;
                    }
                }

                dt.m_rgValidRangeByCustomer.Remove(kv.Key);
                dt.m_rgValidRangeByCustomer.Add(kv.Key, new Tuple<int, int>(nStartIdx, nEndIdx));
                nCustomerIdx++;
            }

            int nCount = 0;
            foreach (KeyValuePair<int, DataRecordCollection> kv in dt.m_rgRecordsByCustomer)
            {
                if (nCount == 0)
                    nCount = kv.Value.Count;

                if (nCount != kv.Value.Count)
                    throw new Exception("The number of records for each customer must be the same!");

                kv.Value.SetStart(dtEarliestStart);
            }

            return dt;
        }

        public bool Normalize(Stopwatch sw, Log log, CancelEvent evtCancel)
        {
            sw.Restart();

            int nIdx = 0;

            foreach (KeyValuePair<int, DataRecordCollection> kv in m_rgRecordsByCustomer)
            {
                double dfMean;
                double dfStdev;

                kv.Value.CalculateStatistics(DataRecord.FIELD.LOG_POWER_USAGE, out dfMean, out dfStdev, true);
                kv.Value.Normalize(DataRecord.FIELD.LOG_POWER_USAGE, DataRecord.FIELD.NORMALIZED_POWER_USAGE, dfMean, dfStdev);

                kv.Value.CalculateStatistics(DataRecord.FIELD.HOUR, out dfMean, out dfStdev);
                kv.Value.Normalize(DataRecord.FIELD.HOUR, DataRecord.FIELD.NORMALIZED_HOUR, dfMean, dfStdev);

                kv.Value.CalculateStatistics(DataRecord.FIELD.HOURS_FROM_START, out dfMean, out dfStdev);
                kv.Value.Normalize(DataRecord.FIELD.HOURS_FROM_START, DataRecord.FIELD.NORMALIZED_HOURS_FROM_START, dfMean, dfStdev);

                if (sw.Elapsed.TotalMilliseconds > 1000)
                {
                    sw.Restart();

                    double dfPct = (double)nIdx / m_rgRecordsByCustomer.Count;
                    log.WriteLine("Normalizing data at " + dfPct.ToString("P") + " complete.");

                    if (evtCancel.WaitOne(0))
                        return false;
                }

                nIdx++;
            }

            return true;
        }
    }
}