using MyCaffe.basecode;
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
            m_data.StartTime = DateTime.MinValue;
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
                        if (m_data.StartTime == DateTime.MinValue)
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
            if (!saveSchemaFile(strPath1, strSub))
                return false;

            return false;
        }

        private bool saveObservedNumericFile(string strPath, string strSub)
        {
            int nCustomers = m_rgCustomers.Count;
            int nRecords = m_data.RecordsPerCustomer;
            int nFields = 3;
            float[] rgData = new float[nCustomers * nRecords * nFields];
            float[] rgCustomer = new float[nRecords * nFields];
            int nIdxCustomer = 0;

            foreach (KeyValuePair<int, List<DataRecord>> kv in m_data.RecordsByCustomer)
            {
                int nIdx = 0;

                foreach (DataRecord rec in kv.Value)
                {
                    rgData[nIdx] = ((DateTimeOffset)rec.Date).ToUnixTimeSeconds();
                    rgData[nIdx + nRecords] = rec.CustomerID;
                    rgData[nIdx + nRecords * 2] = (float)rec.NormalizedPowerUsage;
                    nIdx++;
                }

                Array.Copy(rgCustomer, 0, rgData, rgCustomer.Length * nIdxCustomer, rgCustomer.Length);
                nIdxCustomer++;
            }

            Blob<float>.SaveToNumpy(strPath + "\\" + strSub + "_observed_num.npy", rgData, new int[] { nCustomers, nFields, nRecords });
            return true;
        }

        private bool saveKnownNumericFile(string strPath, string strSub)
        {
            int nCustomers = m_rgCustomers.Count;
            int nRecords = m_data.RecordsPerCustomer;
            int nFields = 4;
            DateTime dtStart = m_data.StartTime;
            float[] rgData = new float[nCustomers * nRecords * nFields];
            float[] rgCustomer = new float[nRecords * nFields];
            int nIdxCustomer = 0;

            foreach (KeyValuePair<int, List<DataRecord>> kv in m_data.RecordsByCustomer)
            {
                int nIdx = 0;

                foreach (DataRecord rec in kv.Value)
                {
                    TimeSpan ts = rec.Date - dtStart;

                    rgData[nIdx] = ((DateTimeOffset)rec.Date).ToUnixTimeSeconds();
                    rgData[nIdx + nRecords] = rec.CustomerID;
                    rgData[nIdx + nRecords * 2] = rec.Date.Hour;
                    rgData[nIdx + nRecords * 3] = (float)ts.TotalHours;
                    nIdx++;
                }

                Array.Copy(rgCustomer, 0, rgData, rgCustomer.Length * nIdxCustomer, rgCustomer.Length);
                nIdxCustomer++;
            }

            Blob<float>.SaveToNumpy(strPath + "\\" + strSub + "_known_num.npy", rgData, new int[] { nCustomers, nFields, nRecords });
            return true;
        }

        private bool saveStaticCategoricalFile(string strPath, string strSub)
        {
            int nCustomers = m_rgCustomers.Count;
            int nRecords = m_data.RecordsPerCustomer;
            int nFields = 2;
            DateTime dtStart = m_data.StartTime;
            float[] rgData = new float[nCustomers * nRecords * nFields];
            float[] rgCustomer = new float[nRecords * nFields];
            int nIdxCustomer = 0;

            foreach (KeyValuePair<int, List<DataRecord>> kv in m_data.RecordsByCustomer)
            {
                int nIdx = 0;

                foreach (DataRecord rec in kv.Value)
                {
                    TimeSpan ts = rec.Date - dtStart;

                    rgData[nIdx] = ((DateTimeOffset)rec.Date).ToUnixTimeSeconds();
                    rgData[nIdx + nRecords] = rec.CustomerID;
                }

                Array.Copy(rgCustomer, 0, rgData, rgCustomer.Length * nIdxCustomer, rgCustomer.Length);
                nIdxCustomer++;
            }

            Blob<float>.SaveToNumpy(strPath + "\\" + strSub + "_static_cat.npy", rgData, new int[] { nCustomers, nFields, nRecords });
            return true;
        }

        private void saveObserved(XmlTextWriter tw)
        {
            tw.WriteStartElement("Observed");
                tw.WriteStartElement("Numeric");
                    tw.WriteElementString("File", "observed_num.npy");
                    
                    tw.WriteStartElement("Field");
                        tw.WriteAttributeString("Index", "0");
                        tw.WriteAttributeString("DateType", "REAL");
                        tw.WriteAttributeString("InputType", "TIME");
                        tw.WriteValue("time");
                    tw.WriteEndElement();

                    tw.WriteStartElement("Field");
                        tw.WriteAttributeString("Index", "1");
                        tw.WriteAttributeString("DateType", "REAL");
                        tw.WriteAttributeString("InputType", "ID");
                        tw.WriteValue("customer id");
                    tw.WriteEndElement();

                    tw.WriteStartElement("Field");
                        tw.WriteAttributeString("Index", "2");
                        tw.WriteAttributeString("DateType", "REAL");
                        tw.WriteAttributeString("InputType", "TARGET");
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
                        tw.WriteAttributeString("DateType", "REAL");
                        tw.WriteAttributeString("InputType", "TIME");
                        tw.WriteValue("time");
                    tw.WriteEndElement();

                    tw.WriteStartElement("Field");
                        tw.WriteAttributeString("Index", "1");
                        tw.WriteAttributeString("DateType", "REAL");
                        tw.WriteAttributeString("InputType", "ID");
                        tw.WriteValue("customer id");
                    tw.WriteEndElement();

                    tw.WriteStartElement("Field");
                        tw.WriteAttributeString("Index", "2");
                        tw.WriteAttributeString("DateType", "REAL");
                        tw.WriteAttributeString("InputType", "KNOWN");
                        tw.WriteAttributeString("DerivedFrom", "TIME:0");
                        tw.WriteAttributeString("Calculation", "HR");
                        tw.WriteValue("hour");
                    tw.WriteEndElement();

                    tw.WriteStartElement("Field");
                        tw.WriteAttributeString("Index", "3");
                        tw.WriteAttributeString("DateType", "REAL");
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
                    tw.WriteElementString("File", "static_num.npy");

                    tw.WriteStartElement("Field");
                        tw.WriteAttributeString("Index", "0");
                        tw.WriteAttributeString("DateType", "REAL");
                        tw.WriteAttributeString("InputType", "TIME");
                        tw.WriteValue("time");
                    tw.WriteEndElement();

                    tw.WriteStartElement("Field");
                        tw.WriteAttributeString("Index", "1");
                        tw.WriteAttributeString("DateType", "REAL");
                        tw.WriteAttributeString("InputType", "STATIC");
                        tw.WriteValue("customer id");
                    tw.WriteEndElement();
                tw.WriteEndElement();

                tw.WriteStartElement("Categorical");
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

        private bool saveSchemaFile(string strPath, string strSub)
        {
            using (XmlTextWriter tw = new XmlTextWriter(strPath + "\\electricity_" + strSub + "_schema.xml", null))
            {
                tw.WriteStartDocument();
                    tw.WriteStartElement("Schema");
                        tw.WriteStartElement("Data");
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

    public class DataRecord
    {
        int m_nCustomerID;
        DateTime m_dt;
        double m_dfPowerUsage;
        double m_dfLogPowerUsage;
        double m_dfNormalizedPowerUsage;

        public DataRecord(int nCustomerID, DateTime dt, double dfPowerUsage)
        {
            m_nCustomerID = nCustomerID;
            m_dt = dt;
            m_dfPowerUsage = dfPowerUsage;
            m_dfLogPowerUsage = (dfPowerUsage == 0) ? 0 : Math.Log(dfPowerUsage);
        }

        public bool IsValid
        {
            get { return (m_dfPowerUsage > 0) ? true : false; }
        }

        public int CustomerID
        {
            get { return m_nCustomerID; }
        }

        public DateTime Date
        {
            get { return m_dt; }
        }

        public double PowerUsage
        {
            get { return m_dfPowerUsage; }
        }

        public double LogPowerUsage
        {
            get { return m_dfLogPowerUsage; }
        }

        public double NormalizedPowerUsage
        {
            get { return m_dfNormalizedPowerUsage; }
            set { m_dfNormalizedPowerUsage = value; }
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append(m_dt.ToString());
            sb.Append(' ');
            sb.Append(IsValid ? "VALID" : "INVALID");
            sb.Append(' ');
            sb.Append("pwr = ");
            sb.Append(m_dfPowerUsage.ToString());
            sb.Append(" logpwr = ");
            sb.Append(m_dfLogPowerUsage.ToString());
            sb.Append(" normpwr = ");
            sb.Append(m_dfNormalizedPowerUsage.ToString());

            return sb.ToString();
        }
    }

    public class DataTable
    {
        int m_nCount = 0;
        Dictionary<int, List<DataRecord>> m_rgRecordsByCustomer = new Dictionary<int, List<DataRecord>>();
        Dictionary<int, double> m_rgTotalsByCustomer = new Dictionary<int, double>();
        Dictionary<int, Tuple<int, int>> m_rgValidRangeByCustomer = new Dictionary<int, Tuple<int, int>>();
        DateTime m_dtStart = DateTime.MinValue;

        public DataTable()
        {
        }

        public DataTable Split(double dfPctStart, double dfPctEnd)
        {
            DataTable dt = new DataTable();
            foreach (KeyValuePair<int, List<DataRecord>> kv in m_rgRecordsByCustomer)
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
                m_rgRecordsByCustomer.Add(rec.CustomerID, new List<DataRecord>());

            m_rgRecordsByCustomer[rec.CustomerID].Add(rec);

            m_nCount++;
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
            get { return m_rgRecordsByCustomer.First().Value.Select(p => ((DateTimeOffset)p.Date).ToUnixTimeSeconds()).ToArray(); }
        }

        public DateTime StartTime
        {
            get { return m_dtStart; }
            set { m_dtStart = value; }
        }

        public Dictionary<int, List<DataRecord>> RecordsByCustomer
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
            DataTable dt = new DataTable();

            sw.Restart();

            int nCustomerIdx = 0;
            foreach (KeyValuePair<int, List<DataRecord>> kv in m_rgRecordsByCustomer)
            {
                int nStartIdx = -1;
                int nEndIdx = 0;
                List<DataRecord> rgRecs = kv.Value.OrderBy(p => p.Date).ToList();

                DateTime dtStart = rgRecs[0].Date;
                dtStart = new DateTime(dtStart.Year, dtStart.Month, dtStart.Day, dtStart.Hour, 0, 0);
                DateTime dtEnd = dtStart + TimeSpan.FromHours(1);
                double dfCustomerTotalPower = 0;

                int nIdx = 0;
                while (nIdx < rgRecs.Count)
                {
                    double dfTotal = 0;
                    while (nIdx < rgRecs.Count && rgRecs[nIdx].Date < dtEnd)
                    {
                        dfTotal += rgRecs[nIdx].PowerUsage;
                        nIdx++;
                    }

                    DataRecord rec = new DataRecord(kv.Key, dtStart, (long)dfTotal);
                    dt.Add(rec);

                    if (rec.IsValid)
                    {
                        if (nStartIdx == -1)
                            nStartIdx = dt.m_rgRecordsByCustomer[kv.Key].Count - 1;

                        nEndIdx = dt.m_rgRecordsByCustomer[kv.Key].Count - 1;
                    }

                    dfCustomerTotalPower += rec.LogPowerUsage;

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

                if (dfCustomerTotalPower > 0)
                    dt.m_rgTotalsByCustomer.Add(kv.Key, dfCustomerTotalPower);
                nCustomerIdx++;
            }

            int nCount = 0;
            foreach (KeyValuePair<int, List<DataRecord>> kv in m_rgRecordsByCustomer)
            {
                if (nCount == 0)
                    nCount = kv.Value.Count;

                if (nCount != kv.Value.Count)
                    throw new Exception("The number of records for each customer must be the same!");
            }

            return dt;
        }

        public bool Normalize(Stopwatch sw, Log log, CancelEvent evtCancel)
        {
            sw.Restart();

            int nIdx = 0;
            int nTotal = m_rgRecordsByCustomer.Sum(p => p.Value.Count);

            foreach (KeyValuePair<int, List<DataRecord>> kv in m_rgRecordsByCustomer)
            {
                int nValidCount = kv.Value.Count(p => p.IsValid);
                double dfTotal = m_rgTotalsByCustomer[kv.Key];
                double dfMean = dfTotal / nValidCount;
                double dfSumDiffSq = 0;

                for (int i = 0; i < kv.Value.Count; i++)
                {
                    if (kv.Value[i].PowerUsage == 0)
                        continue;

                    dfSumDiffSq += Math.Pow(kv.Value[i].LogPowerUsage - dfMean, 2);
                }

                double dfStdDev = Math.Sqrt(dfSumDiffSq / nValidCount);

                for (int i = 0; i < kv.Value.Count; i++)
                {
                    double dfNormVal = 0;

                    if (kv.Value[i].PowerUsage > 0)
                        dfNormVal = (kv.Value[i].LogPowerUsage - dfMean) / dfStdDev;

                    kv.Value[i].NormalizedPowerUsage = dfNormVal;
                    nIdx++;

                    if (sw.Elapsed.TotalMilliseconds > 1000)
                    {
                        sw.Restart();
                        double dfPct = (double)nIdx / (double)nTotal;
                        log.WriteLine("Normalizing data at " + dfPct.ToString("P") + " complete.");

                        if (evtCancel.WaitOne(0))
                            return false;
                    }
                }
            }

            return true;
        }
    }
}
