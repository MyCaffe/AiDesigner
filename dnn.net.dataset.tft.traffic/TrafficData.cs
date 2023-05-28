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
using System.Security.Permissions;
using System.Text;
using System.Threading.Tasks;
using System.Xml;

namespace DNN.net.dataset.tft.traffic
{
    public class TrafficData
    {
        string m_strDataTrainFile;
        string m_strLabelTrainFile;
        string m_strDataTestFile;
        string m_strLabelTestFile;
        string m_strRandPermFile;
        string m_strStationListFile;
        DataTable m_data = new DataTable();
        Dictionary<int, string> m_rgStations = new Dictionary<int, string>();
        Log m_log;
        CancelEvent m_evtCancel;
        Stopwatch m_sw = new Stopwatch();


        public TrafficData(string strDataTrainFile, string strLabelTrainFile, string strDataTestFile, string strLabelTestFile, string strRandPermFile, string strStationListFile, Log log, CancelEvent evtCancel)
        {
            m_strDataTrainFile = strDataTrainFile;
            m_strLabelTrainFile = strLabelTrainFile;
            m_strDataTestFile = strDataTestFile;
            m_strLabelTestFile = strLabelTestFile;
            m_strRandPermFile = strRandPermFile;
            m_strStationListFile = strStationListFile;
            m_log = log;
            m_evtCancel = evtCancel;
        }

        public TrafficData SplitData(double dfPctStart, double dfPctEnd)
        {
            TrafficData data = new TrafficData(null, null, null, null, null, null, m_log, m_evtCancel);

            data.m_rgStations = m_rgStations;
            data.m_data = m_data.Split(dfPctStart, dfPctEnd);

            return data;
        }

        public bool LoadData()
        {
            m_log.WriteLine("Loading raw data...");

            List<List<float[]>> rgData1 = loadFileMatrix(m_strDataTrainFile);
            if (rgData1 == null)
                return false;

            List<float[]> rgLabel1 = loadFileList(m_strLabelTrainFile);
            if (rgLabel1 == null)
                return false;

            List<List<float[]>> rgData = loadFileMatrix(m_strDataTestFile, rgData1);
            if (rgData == null)
                return false;

            List<float[]> rgLabel2 = loadFileList(m_strLabelTestFile);
            if (rgLabel2 == null)
                return false;

            List<float> rgLabel = new List<float>();
            rgLabel.AddRange(rgLabel1[0]);
            rgLabel.AddRange(rgLabel2[0]);

            List<float[]> rgRandPermData = loadFileList(m_strRandPermFile, null, -1);
            if (rgRandPermData == null)
                return false;

            List<float[]> rgStationListData = loadFileList(m_strStationListFile);
            if (rgStationListData == null)
                return false;

            // Load station labels.
            m_rgStations = new Dictionary<int, string>();
            for (int i = 0; i < rgStationListData[0].Length; i++)
            {
                int nStation = (int)rgStationListData[0][i];
                m_rgStations.Add(i, nStation.ToString());
            }

            m_log.WriteLine("Resampling by hour...");

            m_sw.Restart();
            int nIdx = 0;
            int nTotal = rgData.Count * rgData[0].Count * rgData[0][0].Length;

            for (int i = 0; i < rgData.Count; i++)
            {
                for (int j = 0; j < rgData[i].Count; j++)
                {
                    int nLen = rgData[i][j].Length / 6; // recordings each 10 minutes, 6 per hour.
                    float[] rgDst = new float[nLen];

                    for (int k = 0; k < rgDst.Length; k++)
                    {
                        for (int n = 0; n < 6; n++)
                        {
                            rgDst[k] += rgData[i][j][k * 6 + n];
                        }

                        rgDst[k] /= 6.0f;
                        nIdx++;

                        if (m_sw.Elapsed.TotalMilliseconds > 1000)
                        {
                            double dfPct = (double)nIdx / (double)nTotal;
                            m_log.WriteLine("Resampling by hour at " + dfPct.ToString("P") + " complete.");
                            m_sw.Restart();

                            if (m_evtCancel.WaitOne(0))
                                return false;
                        }
                    }

                    rgData[i][j] = rgDst;
                }
            }

            // Reorder the data.
            List<Tuple<int, int, List<float[]>>> rgDataSort = new List<Tuple<int, int, List<float[]>>>();
            for (int i = 0; i < rgData.Count; i++)
            {
                rgDataSort.Add(new Tuple<int, int, List<float[]>>((int)rgRandPermData[0][i], (int)rgLabel[i], rgData[i]));
            }

            rgDataSort = rgDataSort.OrderBy(p => p.Item1).ToList();

            // Load the data table
            m_log.WriteLine("Load the data table...");
            DateTime dtStart = new DateTime(2011, 1, 1);

            m_sw.Restart();

            for (int i = 0; i < rgDataSort.Count; i++)
            {
                int nSensorDay = (int)rgDataSort[i].Item2;

                for (int j = 0; j < rgDataSort[i].Item3.Count; j++)
                {
                    int nStationID = j;

                    for (int k = 0; k < rgDataSort[i].Item3[j].Length; k++)
                    {
                        int nHour = k;

                        DateTime dt = dtStart.AddHours(nHour + i * 24);
                        DataRecord rec = new DataRecord(nStationID, dt, nSensorDay, rgData[i][j][k], dtStart);
                        m_data.Add(rec);
                    }
                }

                if (m_sw.Elapsed.TotalMilliseconds > 1000)
                {
                    double dfPct = (double)i / (double)rgData.Count;
                    m_log.WriteLine("Loading data table at " + dfPct.ToString("P") + " complete.");
                    m_sw.Restart();

                    if (m_evtCancel.WaitOne(0))
                        return false;
                }
            }

            return true;
        }

        private List<float[]> loadFileList(string strFile, List<float[]> rgData = null, int nOffset = 0)
        {
            if (rgData == null)
                rgData = new List<float[]>();

            m_data.StartTime = DateTime.MaxValue;
            m_data.Clear();
            m_rgStations.Clear();

            FileInfo fi = new FileInfo(strFile);
            long lTotalLen = fi.Length;
            long lIdx = 0;
            int nLine = 0;

            m_sw.Restart();

            using (StreamReader sr = new StreamReader(strFile))
            {
                string strLine = sr.ReadLine();
                while (!string.IsNullOrEmpty(strLine))
                {
                    lIdx += strLine.Length;

                    strLine = strLine.Trim('[', ']');
                    string[] rgstr = strLine.Split(' ');

                    float[] rgDataItems = new float[rgstr.Length];
                    for (int i = 0; i < rgstr.Length; i++)
                    {
                        float fVal;
                        if (!float.TryParse(rgstr[i], out fVal))
                            throw new Exception(strFile + ": line (" + nLine.ToString() + ") - Could not parse '" + rgstr[i] + "' to float!");

                        rgDataItems[i] = fVal + nOffset;
                    }

                    rgData.Add(rgDataItems);

                    strLine = sr.ReadLine();

                    if (m_sw.Elapsed.TotalMilliseconds > 1000)
                    {
                        m_sw.Restart();

                        if (m_evtCancel.WaitOne(0))
                            return null;

                        double dfPct = (double)lIdx / (double)lTotalLen;
                        m_log.WriteLine("Loading data file at " + dfPct.ToString("P") + " complete.");
                    }

                    nLine++;
                }
            }

            return rgData;
        }

        private List<List<float[]>> loadFileMatrix(string strFile, List<List<float[]>> rgData = null)
        {
            if (rgData == null)
                rgData = new List<List<float[]>>();

            m_data.StartTime = DateTime.MaxValue;
            m_data.Clear();
            m_rgStations.Clear();

            FileInfo fi = new FileInfo(strFile);
            long lTotalLen = fi.Length;
            long lIdx = 0;
            int nLine = 0;

            m_sw.Restart();

            using (StreamReader sr = new StreamReader(strFile))
            {
                string strLine = sr.ReadLine();
                while (!string.IsNullOrEmpty(strLine))
                {
                    lIdx += strLine.Length;

                    strLine = strLine.Trim('[', ']');
                    string[] rgstr = strLine.Split(';');

                    List<float[]> rgData1 = new List<float[]>();

                    for (int i = 0; i < rgstr.Length; i++)
                    {
                        string[] rgstrItems = rgstr[i].Split(new char[] { ' ' });

                        float[] rgDataItems = new float[rgstrItems.Length];
                        for (int j = 0; j < rgstrItems.Length; j++)
                        {
                            float fVal;
                            if (!float.TryParse(rgstrItems[j], out fVal))
                                throw new Exception(strFile + ": line (" + nLine.ToString() + ") - Could not parse '" + rgstr[i] + "' to float!");

                            rgDataItems[j] = fVal;
                        }

                        rgData1.Add(rgDataItems);
                    }

                    rgData.Add(rgData1);

                    strLine = sr.ReadLine();

                    if (m_sw.Elapsed.TotalMilliseconds > 1000)
                    {
                        m_sw.Restart();

                        if (m_evtCancel.WaitOne(0))
                            return null;

                        double dfPct = (double)lIdx / (double)lTotalLen;
                        m_log.WriteLine("Loading data file at " + dfPct.ToString("P") + " complete.");
                    }

                    nLine++;
                }
            }

            return rgData;
        }

        public bool NormalizeData(Dictionary<DataRecord.FIELD, Tuple<double, double>> rgScalers)
        {
            m_sw.Restart();
            return m_data.Normalize(m_sw, m_log, m_evtCancel, rgScalers);
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
            int nStations = m_rgStations.Count;
            int nRecords = m_data.RecordsPerCustomer;
            int nFields = 2;
            long[] rgData = new long[nStations * nRecords * nFields];
            long[] rgStations = new long[nRecords * nFields];
            int nIdxStation = 0;

            foreach (KeyValuePair<int, DataRecordCollection> kv in m_data.RecordsByCustomer)
            {
                int nIdx = 0;

                foreach (DataRecord rec in kv.Value.Items)
                {
                    rgStations[nIdx++] = ((DateTimeOffset)rec.Date).ToUnixTimeSeconds();
                    rgStations[nIdx++] = rec.StationID;
                }

                Array.Copy(rgStations, 0, rgData, rgStations.Length * nIdxStation, rgStations.Length);
                nIdxStation++;
            }

            Blob<float>.SaveToNumpy(strPath + "\\" + strSub + "_sync.npy", rgData, new int[] { nStations, nRecords, nFields });
            return true;
        }

        private bool saveObservedNumericFile(string strPath, string strSub)
        {
            int nStations = m_rgStations.Count;
            int nRecords = m_data.RecordsPerCustomer;
            int nFields = 1;
            float[] rgData = new float[nStations * nRecords * nFields];
            float[] rgStation = new float[nRecords * nFields];
            int nIdxStation = 0;

            foreach (KeyValuePair<int, DataRecordCollection> kv in m_data.RecordsByCustomer)
            {
                int nIdx = 0;

                foreach (DataRecord rec in kv.Value.Items)
                {
                    rgStation[nIdx++] = (float)rec.NormalizedValue;
                }

                Array.Copy(rgStation, 0, rgData, rgStation.Length * nIdxStation, rgStation.Length);
                nIdxStation++;
            }

            Blob<float>.SaveToNumpy(strPath + "\\" + strSub + "_observed_num.npy", rgData, new int[] { nStations, nRecords, nFields });
            return true;
        }

        private bool saveKnownNumericFile(string strPath, string strSub)
        {
            int nStations = m_rgStations.Count;
            int nRecords = m_data.RecordsPerCustomer;
            int nFields = 4;
            float[] rgData = new float[nStations * nRecords * nFields];
            float[] rgStations = new float[nRecords * nFields];
            int nIdxStation = 0;

            foreach (KeyValuePair<int, DataRecordCollection> kv in m_data.RecordsByCustomer)
            {
                int nIdx = 0;

                foreach (DataRecord rec in kv.Value.Items)
                {
                    rgStations[nIdx++] = (float)rec.NormalizedSensorDay;
                    rgStations[nIdx++] = (float)rec.NormalizedTimeOnDay;
                    rgStations[nIdx++] = (float)rec.NormalizedDayOfWeek;
                    rgStations[nIdx++] = (float)rec.NormalizedHourFromStart;
                }

                Array.Copy(rgStations, 0, rgData, rgStations.Length * nIdxStation, rgStations.Length);
                nIdxStation++;
            }

            Blob<float>.SaveToNumpy(strPath + "\\" + strSub + "_known_num.npy", rgData, new int[] { nStations, nRecords, nFields });
            return true;
        }

        private bool saveStaticCategoricalFile(string strPath, string strSub)
        {
            int nStations = m_rgStations.Count;
            int nFields = 1;
            long[] rgData = new long[nStations * nFields];
            int nIdxStation = 0;

            foreach (KeyValuePair<int, DataRecordCollection> kv in m_data.RecordsByCustomer)
            {
                rgData[nIdxStation] = kv.Value[0].StationID;
                nIdxStation++;
            }

            Blob<float>.SaveToNumpy(strPath + "\\" + strSub + "_static_cat.npy", rgData, new int[] { nStations, nFields });
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
                    tw.WriteValue("station id");
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
                        tw.WriteAttributeString("InputType", "TARGET");
                        tw.WriteValue("value");
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
                        tw.WriteValue("sensor_day");
                    tw.WriteEndElement();

                    tw.WriteStartElement("Field");
                        tw.WriteAttributeString("Index", "1");
                        tw.WriteAttributeString("DataType", "REAL");
                        tw.WriteAttributeString("InputType", "KNOWN");
                        tw.WriteAttributeString("DerivedFrom", "TIME:0");
                        tw.WriteAttributeString("Calculation", "HR");
                        tw.WriteValue("time_on_day");
                    tw.WriteEndElement();

                    tw.WriteStartElement("Field");
                        tw.WriteAttributeString("Index", "2");
                        tw.WriteAttributeString("DataType", "REAL");
                        tw.WriteAttributeString("InputType", "KNOWN");
                        tw.WriteAttributeString("DerivedFrom", "TIME:0");
                        tw.WriteAttributeString("Calculation", "DOW");
                        tw.WriteValue("day_of_week");
                    tw.WriteEndElement();

                    tw.WriteStartElement("Field");
                        tw.WriteAttributeString("Index", "3");
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
                        tw.WriteValue("station id");
                    tw.WriteEndElement();
                tw.WriteEndElement();
            tw.WriteEndElement();
        }

        private void saveLoookups(XmlTextWriter tw)
        {
            tw.WriteStartElement("Lookup");
                tw.WriteAttributeString("Name", "customer id");
                tw.WriteAttributeString("Type", "Categorical");

                foreach (KeyValuePair<int, string> kv in m_rgStations)
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

        public void SortByDate()
        {
            m_rgItems = m_rgItems.OrderBy(p => p.Date).ToList();
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

        public void Normalize(DataRecord.FIELD dt, double dfMean, double dfStdev)
        {
            for (int i = 0; i < m_rgItems.Count; i++)
            {
                if (m_rgItems[i].IsValid)
                {
                    double dfVal = m_rgItems[i].Item(dt);
                    dfVal = (dfVal - dfMean) / dfStdev;
                    m_rgItems[i].NormalizedItem(dt, dfVal);
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
        int m_nStationID;
        DateTime m_dt;
        double[] m_rgFields = new double[5];
        double[] m_rgFieldsNormalized = new double[5];

        public enum FIELD
        {
            VALUE = 0,
            SENSOR_DAY = 1,
            TIME_ON_DAY = 2,
            DAY_OF_WEEK = 3,
            HOURS_FROM_START = 4
        }

        public DataRecord(int nStationID, DateTime dt, int nSensorDay, double dfValue, DateTime dtStart)
        {
            m_nStationID = nStationID;
            m_dt = dt;
            m_rgFields[(int)FIELD.VALUE] = dfValue;
            m_rgFields[(int)FIELD.SENSOR_DAY] = nSensorDay;
            m_rgFields[(int)FIELD.TIME_ON_DAY] = dt.Hour * 60 * 60 + dt.Minute * 60 + dt.Second;
            m_rgFields[(int)FIELD.DAY_OF_WEEK] = (int)dt.DayOfWeek;
            m_rgFields[(int)FIELD.HOURS_FROM_START] = (m_dt - dtStart).TotalHours;
        }

        public bool IsValid
        {
            get { return (Value > 0) ? true : false; }
        }

        public int StationID
        {
            get { return m_nStationID; }
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

        public double NormalizedItem(FIELD field)
        {
            return m_rgFieldsNormalized[(int)field];
        }

        public void NormalizedItem(FIELD field, double dfVal)
        {
            m_rgFieldsNormalized[(int)field] = dfVal;
        }

        public double Value
        {
            get { return m_rgFields[(int)FIELD.VALUE]; }
        }

        public double NormalizedValue
        {
            get { return m_rgFieldsNormalized[(int)FIELD.VALUE]; }
            set { m_rgFieldsNormalized[(int)FIELD.VALUE] = value; }
        }

        public double SensorDay
        {
            get { return m_rgFields[(int)FIELD.SENSOR_DAY]; }
        }

        public double NormalizedSensorDay
        {
            get { return m_rgFieldsNormalized[(int)FIELD.SENSOR_DAY]; }
            set { m_rgFieldsNormalized[(int)FIELD.SENSOR_DAY] = value; }
        }

        public double TimeOnDay
        {
            get { return m_rgFields[(int)FIELD.TIME_ON_DAY]; }
        }

        public double NormalizedTimeOnDay
        {
            get { return m_rgFieldsNormalized[(int)FIELD.TIME_ON_DAY]; }
            set { m_rgFieldsNormalized[(int)FIELD.TIME_ON_DAY] = value; }
        }

        public double DayOfWeek
        {
            get { return m_rgFields[(int)FIELD.DAY_OF_WEEK]; }
        }

        public double NormalizedDayOfWeek
        {
            get { return m_rgFieldsNormalized[(int)FIELD.DAY_OF_WEEK]; }
            set { m_rgFieldsNormalized[(int)FIELD.DAY_OF_WEEK] = value; }
        }

        public double HoursFromStart
        {
            get { return m_rgFields[(int)FIELD.HOURS_FROM_START]; }
        }

        public double NormalizedHourFromStart
        {
            get { return m_rgFieldsNormalized[(int)FIELD.HOURS_FROM_START]; }
            set { m_rgFieldsNormalized[(int)FIELD.HOURS_FROM_START] = value; }
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append(m_dt.ToString());
            sb.Append(' ');
            sb.Append(IsValid ? "VALID" : "INVALID");
            sb.Append(' ');
            sb.Append("val = ");
            sb.Append(Value.ToString());
            sb.Append(" normval = ");
            sb.Append(NormalizedValue.ToString());

            return sb.ToString();
        }
    }

    public class DataTable
    {
        int m_nCount = 0;
        Dictionary<int, DataRecordCollection> m_rgRecordsByStation = new Dictionary<int, DataRecordCollection>();
        Dictionary<int, Tuple<int, int>> m_rgValidRangeByStation = new Dictionary<int, Tuple<int, int>>();
        DateTime m_dtStart = DateTime.MaxValue;

        public DataTable()
        {
        }

        public DataTable Split(double dfPctStart, double dfPctEnd)
        {
            DataTable dt = new DataTable();
            foreach (KeyValuePair<int, DataRecordCollection> kv in m_rgRecordsByStation)
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

                dt.m_rgValidRangeByStation.Remove(kv.Key);
                dt.m_rgValidRangeByStation.Add(kv.Key, new Tuple<int, int>(nValidStartIdx, nValidEndIDx));
            }

            return dt;
        }

        public void Add(DataRecord rec)
        {
            if (rec.Date < m_dtStart)
                m_dtStart = rec.Date;

            if (!m_rgRecordsByStation.ContainsKey(rec.StationID))
                m_rgRecordsByStation.Add(rec.StationID, new DataRecordCollection());

            m_rgRecordsByStation[rec.StationID].Add(rec);

            m_nCount++;
        }

        public int Columns
        {
            get { return m_rgRecordsByStation.First().Value.Count; }
        }

        public int Count
        {
            get { return m_nCount; }
        }

        public int RecordsPerCustomer
        {
            get { return m_rgRecordsByStation.First().Value.Count; }
        }

        public long[] TimeSync
        {
            get { return m_rgRecordsByStation.First().Value.Items.Select(p => ((DateTimeOffset)p.Date).ToUnixTimeSeconds()).ToArray(); }
        }

        public DateTime StartTime
        {
            get { return m_dtStart; }
            set { m_dtStart = value; }
        }

        public Dictionary<int, DataRecordCollection> RecordsByCustomer
        {
            get { return m_rgRecordsByStation; }
        }

        public Dictionary<int, Tuple<int, int>> ValidRangeByCustomer
        {
            get { return m_rgValidRangeByStation; }
        }

        public void Clear()
        {
            m_rgRecordsByStation.Clear();
        }

        private void normalize(DataRecord.FIELD field, DataRecordCollection col, Dictionary<DataRecord.FIELD, Tuple<double, double>> rgScalers)
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
                col.CalculateStatistics(field, out dfMean, out dfStdev, true);
                rgScalers.Add(field, new Tuple<double, double>(dfMean, dfStdev));
            }

            col.Normalize(field, dfMean, dfStdev);
        }

        public bool Normalize(Stopwatch sw, Log log, CancelEvent evtCancel, Dictionary<DataRecord.FIELD, Tuple<double, double>> rgScalers)
        {
            int nIdx = 0;
            sw.Restart();

            foreach (KeyValuePair<int, DataRecordCollection> kv in m_rgRecordsByStation)
            {
                normalize(DataRecord.FIELD.VALUE, kv.Value, rgScalers);
                normalize(DataRecord.FIELD.SENSOR_DAY, kv.Value, rgScalers);
                normalize(DataRecord.FIELD.TIME_ON_DAY, kv.Value, rgScalers);
                normalize(DataRecord.FIELD.DAY_OF_WEEK, kv.Value, rgScalers);
                normalize(DataRecord.FIELD.HOURS_FROM_START, kv.Value, rgScalers);

                if (sw.Elapsed.TotalMilliseconds > 1000)
                {
                    sw.Restart();

                    double dfPct = (double)nIdx / m_rgRecordsByStation.Count;
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
