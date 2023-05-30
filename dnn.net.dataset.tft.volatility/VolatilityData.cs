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
using System.Xml.Linq;

namespace DNN.net.dataset.tft.volatility
{
    public class VolatilityData
    {
        string m_strDataFile;
        DataTable m_data = new DataTable();
        Dictionary<string, int> m_rgSymbolToRegionMap = new Dictionary<string, int>();
        Category m_symbols = new Category();
        Category m_dayOfWeek = new Category();
        Category m_weekOfYear = new Category();
        Category m_month = new Category();
        Category m_region = new Category();
        Log m_log;
        CancelEvent m_evtCancel;
        Stopwatch m_sw = new Stopwatch();


        public VolatilityData(string strDataFile, Log log, CancelEvent evtCancel)
        {
            m_strDataFile = strDataFile;
            m_log = log;
            m_evtCancel = evtCancel;

            m_region.Add("EMEA");
            m_region.Add("APAC");
            m_region.Add("AMER");

            m_dayOfWeek.Add("Monday");
            m_dayOfWeek.Add("Tuesday");
            m_dayOfWeek.Add("Wednesday");
            m_dayOfWeek.Add("Thursday");
            m_dayOfWeek.Add("Friday");

            for (int i = 1; i <= 52; i++)
            {
                m_weekOfYear.Add(i.ToString());
            }

            m_month.Add("January");
            m_month.Add("February");
            m_month.Add("March");
            m_month.Add("April");
            m_month.Add("May");
            m_month.Add("June");
            m_month.Add("July");
            m_month.Add("August");
            m_month.Add("September");
            m_month.Add("October");
            m_month.Add("November");
            m_month.Add("December");

            m_rgSymbolToRegionMap.Add(".AEX", m_region.GetId("EMEA"));
            m_rgSymbolToRegionMap.Add(".AORD", m_region.GetId("APAC"));
            m_rgSymbolToRegionMap.Add(".BFX", m_region.GetId("EMEA"));
            m_rgSymbolToRegionMap.Add(".BSESN", m_region.GetId("APAC"));
            m_rgSymbolToRegionMap.Add(".BVLG", m_region.GetId("EMEA"));
            m_rgSymbolToRegionMap.Add(".BVSP", m_region.GetId("AMER"));
            m_rgSymbolToRegionMap.Add(".DJI", m_region.GetId("AMER"));
            m_rgSymbolToRegionMap.Add(".FCHI", m_region.GetId("EMEA"));
            m_rgSymbolToRegionMap.Add(".FTMIB", m_region.GetId("EMEA"));
            m_rgSymbolToRegionMap.Add(".FTSE", m_region.GetId("EMEA"));
            m_rgSymbolToRegionMap.Add(".GDAXI", m_region.GetId("EMEA"));
            m_rgSymbolToRegionMap.Add(".GSPTSE", m_region.GetId("AMER"));
            m_rgSymbolToRegionMap.Add(".HSI", m_region.GetId("APAC"));
            m_rgSymbolToRegionMap.Add(".IBEX", m_region.GetId("EMEA"));
            m_rgSymbolToRegionMap.Add(".IXIC", m_region.GetId("AMER"));
            m_rgSymbolToRegionMap.Add(".KS11", m_region.GetId("APAC"));
            m_rgSymbolToRegionMap.Add(".KSE", m_region.GetId("APAC"));
            m_rgSymbolToRegionMap.Add(".MXX", m_region.GetId("AMER"));
            m_rgSymbolToRegionMap.Add(".N225", m_region.GetId("APAC"));
            m_rgSymbolToRegionMap.Add(".NSEI", m_region.GetId("APAC"));
            m_rgSymbolToRegionMap.Add(".OMXC20", m_region.GetId("EMEA"));
            m_rgSymbolToRegionMap.Add(".OMXHPI", m_region.GetId("EMEA"));
            m_rgSymbolToRegionMap.Add(".OMXSPI", m_region.GetId("EMEA"));
            m_rgSymbolToRegionMap.Add(".OSEAX", m_region.GetId("EMEA"));
            m_rgSymbolToRegionMap.Add(".RUT", m_region.GetId("AMER"));
            m_rgSymbolToRegionMap.Add(".SMSI", m_region.GetId("EMEA"));
            m_rgSymbolToRegionMap.Add(".SPX", m_region.GetId("AMER"));
            m_rgSymbolToRegionMap.Add(".SSEC", m_region.GetId("APAC"));
            m_rgSymbolToRegionMap.Add(".SSMI", m_region.GetId("EMEA"));
            m_rgSymbolToRegionMap.Add(".STI", m_region.GetId("APAC"));
            m_rgSymbolToRegionMap.Add(".STOXX50E", m_region.GetId("EMEA"));
        }

        public VolatilityData SplitData(double dfPctStart, double dfPctEnd)
        {
            VolatilityData data = new VolatilityData(null, m_log, m_evtCancel);

            data.m_data = m_data.Split(dfPctStart, dfPctEnd);
            data.m_symbols = m_symbols;

            return data;
        }

        public bool LoadData(DateTime dtStart, DateTime dtEnd)
        {
            m_data.Clear();
            m_symbols.Clear();

            FileInfo fi = new FileInfo(m_strDataFile);
            long lTotalLen = fi.Length;
            long lIdx = 0;

            m_sw.Restart();

            using (StreamReader sr = new StreamReader(m_strDataFile))
            {
                string strLine = sr.ReadLine();
                lIdx += strLine.Length;

                strLine = sr.ReadLine();
                while (!string.IsNullOrEmpty(strLine))
                {
                    string[] rgstr = strLine.Split(',');
                    DataRecord rec = DataRecord.Parse(rgstr, m_symbols, m_rgSymbolToRegionMap);

                    m_data.Add(rec);

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

            m_data.SynchronizeAcrossSymbols(m_sw, m_log, m_evtCancel);
            m_data.SetStartTimes();

            return true;
        }

        public DateTime MinDate
        {
            get { return m_data.MinDate; }
        }

        public DateTime MaxDate
        {
            get { return m_data.MaxDate; }
        }

        public bool NormalizeData(Dictionary<int, Dictionary<DataRecord.FIELD, Tuple<double, double>>> rgScalers)
        {
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

            m_log.WriteLine("Saving known categorical data to numpy files...");
            if (!saveKnownCategoricalFile(strPath1, strSub))
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
            int nSymbols = m_symbols.Count;
            int nRecords = m_data.RecordsPerSymbol;
            int nFields = 2;
            long[] rgData = new long[nSymbols * nRecords * nFields];
            long[] rgSymbol = new long[nRecords * nFields];
            int nIdxSymbol = 0;

            foreach (KeyValuePair<int, DataRecordCollection> kv in m_data.RecordsBySymbol)
            {
                int nIdx = 0;

                foreach (DataRecord rec in kv.Value.Items)
                {
                    rgSymbol[nIdx++] = ((DateTimeOffset)rec.Date).ToUnixTimeSeconds();
                    rgSymbol[nIdx++] = rec.SymbolID;
                }

                Array.Copy(rgSymbol, 0, rgData, rgSymbol.Length * nIdxSymbol, rgSymbol.Length);
                nIdxSymbol++;
            }

            Blob<float>.SaveToNumpy(strPath + "\\" + strSub + "_sync.npy", rgData, new int[] { nSymbols, nRecords, nFields });
            return true;
        }

        private bool saveObservedNumericFile(string strPath, string strSub)
        {
            int nSymbols = m_symbols.Count;
            int nRecords = m_data.RecordsPerSymbol;
            int nFields = 2;
            float[] rgData = new float[nSymbols * nRecords * nFields];
            float[] rgSymbol = new float[nRecords * nFields];
            int nIdxSymbol = 0;

            foreach (KeyValuePair<int, DataRecordCollection> kv in m_data.RecordsBySymbol)
            {
                int nIdx = 0;

                foreach (DataRecord rec in kv.Value.Items)
                {
                    rgSymbol[nIdx++] = (float)rec.NormalizedLogVol;
                    rgSymbol[nIdx++] = (float)rec.NormalizedOpenToClose;
                }

                Array.Copy(rgSymbol, 0, rgData, rgSymbol.Length * nIdxSymbol, rgSymbol.Length);
                nIdxSymbol++;
            }

            Blob<float>.SaveToNumpy(strPath + "\\" + strSub + "_observed_num.npy", rgData, new int[] { nSymbols, nRecords, nFields });
            return true;
        }

        private bool saveKnownNumericFile(string strPath, string strSub)
        {
            int nSymbols = m_symbols.Count;
            int nRecords = m_data.RecordsPerSymbol;
            int nFields = 1;
            float[] rgData = new float[nSymbols * nRecords * nFields];
            float[] rgSymbol = new float[nRecords * nFields];
            int nIdxSymbol = 0;

            foreach (KeyValuePair<int, DataRecordCollection> kv in m_data.RecordsBySymbol)
            {
                int nIdx = 0;

                foreach (DataRecord rec in kv.Value.Items)
                {
                    rgSymbol[nIdx++] = (float)rec.NormalizedDaysFromStart;
                }

                Array.Copy(rgSymbol, 0, rgData, rgSymbol.Length * nIdxSymbol, rgSymbol.Length);
                nIdxSymbol++;
            }

            Blob<float>.SaveToNumpy(strPath + "\\" + strSub + "_known_num.npy", rgData, new int[] { nSymbols, nRecords, nFields });
            return true;
        }

        private bool saveKnownCategoricalFile(string strPath, string strSub)
        {
            int nSymbols = m_symbols.Count;
            int nRecords = m_data.RecordsPerSymbol;
            int nFields = 4;
            long[] rgData = new long[nSymbols * nRecords * nFields];
            long[] rgSymbol = new long[nRecords * nFields];
            int nIdxSymbol = 0;

            foreach (KeyValuePair<int, DataRecordCollection> kv in m_data.RecordsBySymbol)
            {
                int nIdx = 0;

                foreach (DataRecord rec in kv.Value.Items)
                {
                    rgSymbol[nIdx++] = rec.DayOfWeek;
                    rgSymbol[nIdx++] = rec.DayOfMonth;
                    rgSymbol[nIdx++] = rec.WeekOfYear;
                    rgSymbol[nIdx++] = rec.Month;
                }

                Array.Copy(rgSymbol, 0, rgData, rgSymbol.Length * nIdxSymbol, rgSymbol.Length);
                nIdxSymbol++;
            }

            Blob<long>.SaveToNumpy(strPath + "\\" + strSub + "_known_cat.npy", rgData, new int[] { nSymbols, nRecords, nFields });
            return true;
        }

        private bool saveStaticCategoricalFile(string strPath, string strSub)
        {
            int nSymbols = m_symbols.Count;
            int nFields = 1;
            long[] rgData = new long[nSymbols * nFields];
            int nIdxSymbol = 0;

            foreach (KeyValuePair<int, DataRecordCollection> kv in m_data.RecordsBySymbol)
            {
                rgData[nIdxSymbol] = kv.Value[0].RegionID;
                nIdxSymbol++;
            }

            Blob<float>.SaveToNumpy(strPath + "\\" + strSub + "_static_cat.npy", rgData, new int[] { nSymbols, nFields });
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
                    tw.WriteValue("date");
                tw.WriteEndElement();

                tw.WriteStartElement("Field");
                    tw.WriteAttributeString("Index", "1");
                    tw.WriteAttributeString("DataType", "REAL");
                    tw.WriteAttributeString("InputType", "ID");
                    tw.WriteValue("symbol id");
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
                        tw.WriteValue("log vol");
                    tw.WriteEndElement();

                    tw.WriteStartElement("Field");
                        tw.WriteAttributeString("Index", "1");
                        tw.WriteAttributeString("DataType", "REAL");
                        tw.WriteAttributeString("InputType", "OBSERVED");
                        tw.WriteValue("open_to_close");
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
                        tw.WriteAttributeString("Calculation", "DFS");
                        tw.WriteValue("days_from_start");
                    tw.WriteEndElement();
                tw.WriteEndElement();

                tw.WriteStartElement("Categorical");
                    tw.WriteElementString("File", "known_cat.npy");

                    tw.WriteStartElement("Field");
                        tw.WriteAttributeString("Index", "1");
                        tw.WriteAttributeString("DataType", "REAL");
                        tw.WriteAttributeString("InputType", "KNOWN");
                        tw.WriteAttributeString("DerivedFrom", "TIME:0");
                        tw.WriteAttributeString("Calculation", "DOW");
                        tw.WriteValue("day_of_week");
                    tw.WriteEndElement();

                    tw.WriteStartElement("Field");
                        tw.WriteAttributeString("Index", "0");
                        tw.WriteAttributeString("DataType", "REAL");
                        tw.WriteAttributeString("InputType", "KNOWN");
                        tw.WriteAttributeString("DerivedFrom", "TIME:0");
                        tw.WriteAttributeString("Calculation", "DOM");
                        tw.WriteValue("day_of_month");
                    tw.WriteEndElement();

                    tw.WriteStartElement("Field");
                        tw.WriteAttributeString("Index", "1");
                        tw.WriteAttributeString("DataType", "REAL");
                        tw.WriteAttributeString("InputType", "KNOWN");
                        tw.WriteAttributeString("DerivedFrom", "TIME:0");
                        tw.WriteAttributeString("Calculation", "WOY");
                        tw.WriteValue("week_of_year");
                    tw.WriteEndElement();

                    tw.WriteStartElement("Field");
                        tw.WriteAttributeString("Index", "2");
                        tw.WriteAttributeString("DataType", "REAL");
                        tw.WriteAttributeString("InputType", "KNOWN");
                        tw.WriteAttributeString("DerivedFrom", "TIME:0");
                        tw.WriteAttributeString("Calculation", "MO");
                        tw.WriteValue("month");
                    tw.WriteEndElement();
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
                        tw.WriteValue("region");
                    tw.WriteEndElement();
                tw.WriteEndElement();
            tw.WriteEndElement();
        }

        private void saveLoookups(XmlTextWriter tw)
        {
            tw.WriteStartElement("Lookup");
            tw.WriteAttributeString("Name", "customer id");
            tw.WriteAttributeString("Type", "Categorical");

            foreach (int nID in m_symbols.IDs)
            {
                tw.WriteStartElement("Item");
                tw.WriteAttributeString("Index", nID.ToString());
                tw.WriteAttributeString("ValidRangeStartIdx", m_data.ValidRangeBySymbol[nID].Item1.ToString());
                tw.WriteAttributeString("ValidRangeEndIdx", m_data.ValidRangeBySymbol[nID].Item2.ToString());
                tw.WriteValue(nID);
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
        int m_nSymbolID = 0;
        List<DataRecord> m_rgItems = new List<DataRecord>();

        public DataRecordCollection()
        {
        }

        public Tuple<int, int> SynchronizeToDates(List<DateTime> rg)
        {
            int nValidStart = -1;
            int nValidEnd = -1;

            m_rgItems = m_rgItems.OrderBy(p => p.Date).ToList();

            List<DataRecord> rgItems = new List<DataRecord>();
            int nSrcIdx = 0;

            for (int i = 0; i < rg.Count; i++)
            {
                DateTime dt = rg[i];

                if (nSrcIdx < m_rgItems.Count)
                {
                    if (dt < m_rgItems[nSrcIdx].Date)
                        rgItems.Add(m_rgItems[nSrcIdx].Clone(dt));
                    else
                    {
                        if (nValidStart == -1)
                            nValidStart = i;

                        rgItems.Add(m_rgItems[nSrcIdx].Clone());
                    }
                    nSrcIdx++;
                }
                else
                {
                    if (nValidEnd == -1)
                        nValidEnd = i;

                    rgItems.Add(m_rgItems[m_rgItems.Count - 1].Clone(dt));
                }
            }

            if (nValidEnd == -1)
                nValidEnd = m_rgItems.Count - 1;

            m_rgItems = rgItems;

            return new Tuple<int, int>(nValidStart, nValidEnd);
        }

        public void Add(DataRecord rec)
        {
            m_nSymbolID = rec.SymbolID;
            m_rgItems.Add(rec);
        }

        public int SymbolID
        {
            get { return m_nSymbolID; }
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

        public void Normalize(DataRecord.FIELD dt, double dfMean, double dfStdev)
        {
            for (int i = 0; i < m_rgItems.Count; i++)
            {
                if (m_rgItems[i].IsValid)
                {
                    double dfVal = m_rgItems[i].Item(dt);
                    dfVal = (dfVal - dfMean) / dfStdev;
                    m_rgItems[i].ItemNormalized(dt, dfVal);
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
        int m_nSymbolID;
        DateTime m_dt;
        double[] m_rgFields = new double[3];
        double[] m_rgFieldsNormalized = new double[3];
        int m_nRegionID = 0;
        double m_dfOpenToClose;
        double m_dfRkTwoScale;
        double m_dfRkTh2;
        double m_dfMedRv;
        double m_dfRssVSS;
        double m_dfClosePrice;
        double m_dfNobs;
        double m_dfBvSS;
        double m_dfRv10;
        double m_dfRv5SS;
        double m_dfOpenTime;
        double m_dfRv5;
        double m_dfRsv;
        double m_dfRkParzen;
        double m_dfRv10SS;
        double m_dfBv;
        double m_dfOpenPrice;
        double m_dfCloseTime;
        int m_nDayOfWeek;
        int m_nDayOfMonth;
        int m_nWeekOfYear;
        int m_nMonth;

        public enum FIELD
        {
            LOG_VOL,
            OPEN_TO_CLOSE,
            DAYS_FROM_START,
        }

        public DataRecord()
        {
        }

        private static int dayOfYear(DateTime dt)
        {
            DateTime dt1 = new DateTime(dt.Year, 1, 1);
            TimeSpan ts = dt - dt1;
            return ts.Days;
        }

        public DataRecord Clone(DateTime? dt = null)
        {
            if (dt == null)
                dt = m_dt;

            DataRecord rec = new DataRecord();
            rec.m_nSymbolID = m_nSymbolID;
            rec.m_dt = dt.Value;
            rec.m_rgFields = m_rgFields;
            rec.m_rgFieldsNormalized = m_rgFieldsNormalized;
            rec.m_nRegionID = m_nRegionID;
            rec.m_dfOpenToClose = m_dfOpenToClose;
            rec.m_dfRkTwoScale = m_dfRkTwoScale;
            rec.m_dfRkTh2 = m_dfRkTh2;
            rec.m_dfMedRv = m_dfMedRv;
            rec.m_dfRssVSS = m_dfRssVSS;
            rec.m_dfClosePrice = m_dfClosePrice;
            rec.m_dfNobs = m_dfNobs;
            rec.m_dfBvSS = m_dfBvSS;
            rec.m_dfRv10 = m_dfRv10;
            rec.m_dfRv5SS = m_dfRv5SS;
            rec.m_dfOpenTime = m_dfOpenTime;
            rec.m_dfRv5 = m_dfRv5;
            rec.m_dfRsv = m_dfRsv;
            rec.m_dfRkParzen = m_dfRkParzen;
            rec.m_dfRv10SS = m_dfRv10SS;
            rec.m_dfBv = m_dfBv;
            rec.m_dfOpenPrice = m_dfOpenPrice;
            rec.m_dfCloseTime = m_dfCloseTime;
            rec.m_nDayOfWeek = (int)dt.Value.DayOfWeek;
            rec.m_nDayOfMonth = dt.Value.Day - 1;
            rec.m_nMonth = dt.Value.Month - 1;
            rec.m_nWeekOfYear = dayOfYear(dt.Value);
            return rec;
        }

        private static double parse(string str)
        {
            double dfVal;

            if (string.IsNullOrEmpty(str))
                return 0;

            if (!double.TryParse(str, out dfVal))
                return 0;

            return dfVal;
        }

        public static DataRecord Parse(string[] rgstr, Category symbols, Dictionary<string, int> rgRegionMap)
        {
            DataRecord rec = new DataRecord();

            string strSymbol = rgstr[1];
            symbols.Add(strSymbol);

            rec.m_dt = DateTime.Parse(rgstr[0]);
            rec.m_nSymbolID = symbols.Add(strSymbol);
            rec.m_nRegionID = rgRegionMap[strSymbol];
            rec.m_dfOpenToClose = parse(rgstr[2]);
            rec.m_dfRkTwoScale = parse(rgstr[3]);
            rec.m_dfRkTh2 = parse(rgstr[4]);
            rec.m_dfMedRv = parse(rgstr[5]);
            rec.m_dfRssVSS = parse(rgstr[6]);
            rec.m_dfClosePrice = parse(rgstr[7]);
            rec.m_dfNobs = parse(rgstr[8]);
            rec.m_dfBvSS = parse(rgstr[9]);
            rec.m_dfRv10 = parse(rgstr[10]);
            rec.m_dfRv5SS = parse(rgstr[11]);
            rec.m_dfOpenTime = parse(rgstr[12]);
            rec.m_dfRv5 = parse(rgstr[13]);
            rec.m_dfRsv = parse(rgstr[14]);
            rec.m_dfRkParzen = parse(rgstr[15]);
            rec.m_dfRv10SS = parse(rgstr[16]);
            rec.m_dfBv = parse(rgstr[17]);
            rec.m_dfOpenPrice = parse(rgstr[18]);
            rec.m_dfCloseTime = parse(rgstr[19]);
            rec.m_rgFields[(int)FIELD.LOG_VOL] = Math.Log(rec.m_dfRv5SS);
            rec.m_rgFields[(int)FIELD.OPEN_TO_CLOSE] = rec.m_dfOpenToClose;
            rec.m_nDayOfMonth = rec.m_dt.Day;
            rec.m_nDayOfWeek = (int)rec.m_dt.DayOfWeek;
            rec.m_nWeekOfYear = dayOfYear(rec.m_dt);
            rec.m_nMonth = rec.m_dt.Month;

            return rec;
        }

        public void SetStart(DateTime dt)
        {
            m_rgFields[(int)FIELD.DAYS_FROM_START] = (m_dt - dt).TotalDays;
        }

        public bool IsValid
        {
            get { return (LogVol != 0) ? true : false; }
        }

        public int SymbolID
        {
            get { return m_nSymbolID; }
        }

        public int RegionID
        {
            get { return m_nRegionID; }
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

        public double ItemNormalized(FIELD field)
        {
            return m_rgFieldsNormalized[(int)field];
        }

        public void ItemNormalized(FIELD field, double dfVal)
        {
            m_rgFieldsNormalized[(int)field] = dfVal;
        }

        public double LogVol
        {
            get { return m_rgFields[(int)FIELD.LOG_VOL]; }
        }

        public double NormalizedLogVol
        {
            get { return m_rgFieldsNormalized[(int)FIELD.LOG_VOL]; }
            set { m_rgFieldsNormalized[(int)FIELD.LOG_VOL] = value; }
        }

        public double OpenToClose
        {
            get { return m_rgFields[(int)FIELD.OPEN_TO_CLOSE]; }
        }

        public double NormalizedOpenToClose
        {
            get { return m_rgFieldsNormalized[(int)FIELD.OPEN_TO_CLOSE]; }
            set { m_rgFieldsNormalized[(int)FIELD.OPEN_TO_CLOSE] = value; }
        }

        public double DaysFromStart
        {
            get { return m_rgFields[(int)FIELD.DAYS_FROM_START]; }
            set { m_rgFields[(int)FIELD.DAYS_FROM_START] = value; }
        }

        public double NormalizedDaysFromStart
        {
            get { return m_rgFieldsNormalized[(int)FIELD.DAYS_FROM_START]; }
            set { m_rgFieldsNormalized[(int)FIELD.DAYS_FROM_START] = value; }
        }

        public int DayOfWeek
        {
            get { return m_nDayOfWeek; }
        }

        public int DayOfMonth
        {
            get { return m_nDayOfMonth; }
        }

        public int WeekOfYear
        {
            get { return m_nWeekOfYear; }
        }

        public int Month
        {
            get { return m_nMonth; }
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append(m_dt.ToString());
            sb.Append(' ');
            sb.Append(m_nSymbolID.ToString());
            sb.Append(' ');
            sb.Append(IsValid ? "VALID" : "INVALID");
            sb.Append(' ');
            sb.Append("logvol = ");
            sb.Append(LogVol.ToString());
            sb.Append(" normvol = ");
            sb.Append(NormalizedLogVol.ToString());

            return sb.ToString();
        }
    }

    public class Category
    {
        Dictionary<int, string> m_rgItems = new Dictionary<int, string>();
        Dictionary<string, int> m_rgItemToId = new Dictionary<string, int>();

        public Category()
        {
        }

        public void Clear()
        {
            m_rgItems.Clear();
            m_rgItemToId.Clear();
        }

        public int Add(string str)
        {
            if (!m_rgItemToId.ContainsKey(str))
            {
                int nId = m_rgItemToId.Count;
                m_rgItemToId.Add(str, nId);
                m_rgItems.Add(nId, str);
            }

            return m_rgItemToId[str];
        }

        public int Count
        {
            get { return m_rgItems.Count; }
        }

        public List<int> IDs
        {
            get { return m_rgItems.Keys.ToList(); }
        }

        public int GetId(string str)
        {
            if (m_rgItemToId.ContainsKey(str))
                return m_rgItemToId[str];

            return -1;
        }

        public string GetItem(int nId)
        {
            if (m_rgItems.ContainsKey(nId))
                return m_rgItems[nId];

            return null;
        }
    }

    public class DataTable
    {
        int m_nCount = 0;
        Dictionary<DateTime, int> m_rgDates = new Dictionary<DateTime, int>();
        Dictionary<int, DataRecordCollection> m_rgRecordsBySymbol = new Dictionary<int, DataRecordCollection>();
        Dictionary<int, Tuple<int, int>> m_rgValidRangeBySymbol = new Dictionary<int, Tuple<int, int>>();
        DateTime m_dtMin = DateTime.MaxValue;
        DateTime m_dtMax = DateTime.MinValue;

        public DataTable()
        {
        }

        public DataTable Split(double dfPctStart, double dfPctEnd)
        {
            DateTime dtMin = MinDate;
            DateTime dtMax = MaxDate;

            int nMinYear = dtMin.Year;
            int nMaxYear = dtMax.Year;
            int nYearRange = nMaxYear - nMinYear;

            int nYearStart = (int)(nMinYear + (nYearRange * dfPctStart));
            int nYearEnd = (int)(nMinYear + (nYearRange * dfPctEnd));

            if (dfPctEnd == 1)
                nYearEnd++;

            int nValidStart = -1;
            int nValidEnd = -1;
            int nStart = -1;

            DataTable dt = new DataTable();
            foreach (KeyValuePair<int, DataRecordCollection> kv in m_rgRecordsBySymbol)
            {
                for (int i=0; i<kv.Value.Count; i++)
                {
                    DataRecord rec = kv.Value[i];

                    if (rec.IsValid && rec.Date.Year >= nYearStart && rec.Date.Year < nYearEnd)
                    {
                        dt.Add(rec);

                        if (nStart == -1)
                            nStart = i;

                        if (nValidStart == -1 && i >= m_rgValidRangeBySymbol[kv.Key].Item1)
                            nValidStart = i - nStart;

                        nValidEnd = i - nStart;
                    }
                }

                dt.m_rgValidRangeBySymbol.Add(kv.Key, new Tuple<int, int>(nValidStart, nValidEnd));
            }
           
            return dt;
        }

        public void Add(DataRecord rec)
        {
            if (!m_rgRecordsBySymbol.ContainsKey(rec.SymbolID))
                m_rgRecordsBySymbol.Add(rec.SymbolID, new DataRecordCollection());

            m_rgRecordsBySymbol[rec.SymbolID].Add(rec);

            if (rec.Date < m_dtMin)
                m_dtMin = rec.Date;

            if (rec.Date > m_dtMax)
                m_dtMax = rec.Date;

            if (!m_rgDates.ContainsKey(rec.Date))
                m_rgDates.Add(rec.Date, 0);
            m_rgDates[rec.Date]++;

            m_nCount++;
        }

        public bool SynchronizeAcrossSymbols(Stopwatch sw, Log log, CancelEvent evtCancel)
        {
            List<DateTime> rgDates = m_rgDates.Keys.OrderBy(p => p.Date).ToList();

            sw.Restart();

            m_rgValidRangeBySymbol.Clear();
            List<DataRecordCollection> rgCol = m_rgRecordsBySymbol.Values.ToList();

            for (int i = 0; i < rgCol.Count; i++)
            {
                Tuple<int, int> validRange = rgCol[i].SynchronizeToDates(rgDates);

                m_rgValidRangeBySymbol.Add(rgCol[i].SymbolID, validRange);

                if (sw.Elapsed.TotalMilliseconds > 1000)
                {
                    double dfPct = (double)i / rgCol.Count();
                    log.WriteLine("Synchonizing data across dates - " + dfPct.ToString("P") + " complete.");

                    if (evtCancel.WaitOne(0))
                        return false;
                }
            }

            return true;
        }

        public DateTime MinDate
        {
            get { return m_dtMin; }
        }

        public DateTime MaxDate
        {
            get { return m_dtMax; }
        }

        public void SetStartTimes()
        {
            foreach (KeyValuePair<int, DataRecordCollection> kv in m_rgRecordsBySymbol)
            {
                DataRecordCollection col = kv.Value;
                for (int i = 0; i < col.Count; i++)
                {
                    DataRecord rec = col[i];
                    rec.SetStart(m_dtMin);
                }
            }
        }

        public int Columns
        {
            get { return m_rgRecordsBySymbol.First().Value.Count; }
        }

        public int Count
        {
            get { return m_nCount; }
        }

        public int RecordsPerSymbol
        {
            get { return m_rgRecordsBySymbol.First().Value.Count; }
        }

        public long[] TimeSync
        {
            get { return m_rgRecordsBySymbol.First().Value.Items.Select(p => ((DateTimeOffset)p.Date).ToUnixTimeSeconds()).ToArray(); }
        }

        public DateTime StartTime
        {
            get { return m_dtMin; }
            set { m_dtMin = value; }
        }

        public Dictionary<int, DataRecordCollection> RecordsBySymbol
        {
            get { return m_rgRecordsBySymbol; }
        }

        public Dictionary<int, Tuple<int, int>> ValidRangeBySymbol
        {
            get { return m_rgValidRangeBySymbol; }
        }

        public void Clear()
        {
            m_rgRecordsBySymbol.Clear();
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

        public bool Normalize(Stopwatch sw, Log log, CancelEvent evtCancel, Dictionary<int, Dictionary<DataRecord.FIELD, Tuple<double, double>>> rgScalers)
        {
            sw.Restart();

            int nIdx = 0;

            foreach (KeyValuePair<int, DataRecordCollection> kv in m_rgRecordsBySymbol)
            {
                Dictionary<DataRecord.FIELD, Tuple<double, double>> rgScalers1;

                if (!rgScalers.ContainsKey(kv.Key))
                    rgScalers.Add(kv.Key, new Dictionary<DataRecord.FIELD, Tuple<double, double>>());

                rgScalers1 = rgScalers[kv.Key];

                normalize(DataRecord.FIELD.LOG_VOL, kv.Value, rgScalers1);
                normalize(DataRecord.FIELD.OPEN_TO_CLOSE, kv.Value, rgScalers1);
                normalize(DataRecord.FIELD.DAYS_FROM_START, kv.Value, rgScalers1);

                if (sw.Elapsed.TotalMilliseconds > 1000)
                {
                    sw.Restart();

                    double dfPct = (double)nIdx / m_rgRecordsBySymbol.Count;
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
