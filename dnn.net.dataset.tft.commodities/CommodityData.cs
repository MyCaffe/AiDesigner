using MyCaffe.basecode;
using MyCaffe.basecode.descriptors;
using MyCaffe.common;
using MyCaffe.db.temporal;
using SimpleGraphing;
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

namespace dnn.net.dataset.tft.commodity
{
    public class CommodityData
    {
        string m_strDataPath;
        DataTable m_data = new DataTable();
        Dictionary<int, string> m_rgCommodities = new Dictionary<int, string>();
        Dictionary<string, int> m_rgCommodityIDs = new Dictionary<string, int>();
        Log m_log;
        CancelEvent m_evtCancel;
        Stopwatch m_sw = new Stopwatch();


        public CommodityData(string strDataPath, Log log, CancelEvent evtCancel)
        {
            m_strDataPath = strDataPath;
            m_log = log;
            m_evtCancel = evtCancel;
        }

        public Tuple<CommodityData, CommodityData> SplitData(DateTime dtTrainingStart, DateTime dtTrainingEnd, DateTime dtTestingStart, DateTime dtTestingEnd)
        {
            CommodityData dataTrain = new CommodityData(null, m_log, m_evtCancel);
            dataTrain.m_rgCommodities = m_rgCommodities;
            dataTrain.m_rgCommodityIDs = m_rgCommodityIDs;
            dataTrain.m_data = m_data.Split(dtTrainingStart, dtTrainingEnd, null);

            CommodityData dataTest = new CommodityData(null, m_log, m_evtCancel);
            dataTest.m_rgCommodities = m_rgCommodities;
            dataTest.m_rgCommodityIDs = m_rgCommodityIDs;
            dataTest.m_data = m_data.Split(dtTestingStart, dtTestingEnd, dataTrain.m_data);

            return new Tuple<CommodityData, CommodityData>(dataTrain, dataTest);
        }

        private string getCommodity(string strFile)
        {
            return Path.GetFileNameWithoutExtension(strFile);
        }

        public bool LoadData(DateTime dtStart, DateTime dtEnd)
        {
            Dictionary<DateTime, int> rgDates = new Dictionary<DateTime, int>();

            m_log.WriteLine("Loading raw data from '" + m_strDataPath + "'...");
            string[] rgstrFiles = Directory.GetFiles(m_strDataPath, "*.csv");

            m_sw.Restart();

            for (int i = 0; i < rgstrFiles.Length; i++)
            {
                string strCommodity = getCommodity(rgstrFiles[i]);

                if (!m_rgCommodities.ContainsKey(i))
                    m_rgCommodities.Add(i, strCommodity);

                if (!m_rgCommodityIDs.ContainsKey(strCommodity))
                    m_rgCommodityIDs.Add(strCommodity, i);

                double dfLastPrice = 0;
                CalculationArray ca = new CalculationArray(20);
                string[] rgstrLines = File.ReadAllLines(rgstrFiles[i]);
                List<DataRecord> rgRawRecords = new List<DataRecord>();
                int nBadConsecutiveDataCount = 0;
                int nMaxBadConsecutiveDataCount = 0;

                for (int j = 1; j < rgstrLines.Length; j++)
                {
                    string[] rgstr = rgstrLines[j].Split(',');
                    if (rgstr.Length == 2)
                    {
                        DateTime dt = DateTime.Parse(rgstr[0]);
                        double dfPrice = dfLastPrice;
                        double.TryParse(rgstr[1], out dfPrice);

                        if (ca.IsFull)
                        {
                            double dfPctChange = (dfPrice - dfLastPrice) / dfLastPrice;

                            if (Math.Abs(dfPctChange) > 0.8)
                            {
                                dfPrice = dfLastPrice;
                                nBadConsecutiveDataCount++;
                            }
                            else
                            {
                                nMaxBadConsecutiveDataCount = Math.Max(nMaxBadConsecutiveDataCount, nBadConsecutiveDataCount);
                                nBadConsecutiveDataCount = 0;
                                ca.Add(dfPrice, dt, false);
                            }
                        }
                        else
                        {
                            ca.Add(dfPrice, dt, false);
                        }

                        DataRecord rec = new DataRecord(i, dt, dfPrice, strCommodity);

                        rgRawRecords.Add(rec);
                        dfLastPrice = dfPrice;
                    }

                    if (nMaxBadConsecutiveDataCount >= 3)
                        break;
                }

                if (nMaxBadConsecutiveDataCount < 3)
                {
                    foreach (DataRecord rec in rgRawRecords)
                    {
                        DateTime dt = rec.Date;

                        m_data.Add(rec);

                        if (!rgDates.ContainsKey(dt))
                            rgDates.Add(dt, 0);
                    }
                }
                else
                {
                    m_log.WriteLine("Skipping '" + strCommodity + "' due to bad data.");
                }

                if (m_sw.Elapsed.TotalMilliseconds > 1000)
                {
                    m_sw.Restart();
                    double dfPct = (double)i / (double)rgstrFiles.Length;
                    m_log.WriteLine("Loading raw data at " + dfPct.ToString("P") + " complete.");
                }
            }

            m_log.WriteLine("A total of " + rgstrFiles.Length.ToString() + " commodities were processed, static ID max = " + rgstrFiles.Length.ToString() + ".");
            m_data.PreProcess(dtStart, dtEnd, m_log, m_evtCancel, rgDates.Keys.OrderBy(p => p).ToList());

            return true;
        }

        public int SaveAsSql(string strName, string strSub)
        {
            DatabaseTemporal db = new DatabaseTemporal();

            m_sw.Start();

            int nIdx = 0;
            int nTotal = m_data.RecordsByCommodity.Sum(p => p.Value.Items.Count);
            int nSrcID = db.AddSource(strName + "." + strSub, m_rgCommodities.Count, 8, m_data.RecordsPerCommodity, true, 0, false);
            int nItemCount = 0;
            int nTotalSteps = m_data.RecordsByCommodity.Max(p => p.Value.Items.Count);

            db.Open(nSrcID);
            db.EnableBulk(true);

            int nOrdering = 0;
            int nStreamID_target_returns = db.AddValueStream(nSrcID, "target_returns", nOrdering++, MyCaffe.basecode.descriptors.ValueStreamDescriptor.STREAM_CLASS_TYPE.TARGET, MyCaffe.basecode.descriptors.ValueStreamDescriptor.STREAM_VALUE_TYPE.NUMERIC, null, null, null, 1, null);
            int nStreamID_norm_daily_returns = db.AddValueStream(nSrcID, "norm_daily_returns", nOrdering++, MyCaffe.basecode.descriptors.ValueStreamDescriptor.STREAM_CLASS_TYPE.OBSERVED, MyCaffe.basecode.descriptors.ValueStreamDescriptor.STREAM_VALUE_TYPE.NUMERIC, null, null, null, 1, null);
            int nStreamID_norm_monthly_returns = db.AddValueStream(nSrcID, "norm_monthly_returns", nOrdering++, MyCaffe.basecode.descriptors.ValueStreamDescriptor.STREAM_CLASS_TYPE.OBSERVED, MyCaffe.basecode.descriptors.ValueStreamDescriptor.STREAM_VALUE_TYPE.NUMERIC, null, null, null, 1, null);
            int nStreamID_norm_quarterly_returns = db.AddValueStream(nSrcID, "norm_quarterly_returns", nOrdering++, MyCaffe.basecode.descriptors.ValueStreamDescriptor.STREAM_CLASS_TYPE.OBSERVED, MyCaffe.basecode.descriptors.ValueStreamDescriptor.STREAM_VALUE_TYPE.NUMERIC, null, null, null, 1, null);
            int nStreamID_norm_biannual_returns = db.AddValueStream(nSrcID, "norm_biannual_returns", nOrdering++, MyCaffe.basecode.descriptors.ValueStreamDescriptor.STREAM_CLASS_TYPE.OBSERVED, MyCaffe.basecode.descriptors.ValueStreamDescriptor.STREAM_VALUE_TYPE.NUMERIC, null, null, null, 1, null);
            int nStreamID_norm_annual_returns = db.AddValueStream(nSrcID, "norm_annual_returns", nOrdering++, MyCaffe.basecode.descriptors.ValueStreamDescriptor.STREAM_CLASS_TYPE.OBSERVED, MyCaffe.basecode.descriptors.ValueStreamDescriptor.STREAM_VALUE_TYPE.NUMERIC, null, null, null, 1, null);
            int nStreamID_macd1 = db.AddValueStream(nSrcID, "macd_8_24", nOrdering++, MyCaffe.basecode.descriptors.ValueStreamDescriptor.STREAM_CLASS_TYPE.OBSERVED, MyCaffe.basecode.descriptors.ValueStreamDescriptor.STREAM_VALUE_TYPE.NUMERIC, null, null, null, 1, null);
            int nStreamID_macd2 = db.AddValueStream(nSrcID, "macd_16_48", nOrdering++, MyCaffe.basecode.descriptors.ValueStreamDescriptor.STREAM_CLASS_TYPE.OBSERVED, MyCaffe.basecode.descriptors.ValueStreamDescriptor.STREAM_VALUE_TYPE.NUMERIC, null, null, null, 1, null);
            int nStreamID_macd3 = db.AddValueStream(nSrcID, "macd_32_96", nOrdering++, MyCaffe.basecode.descriptors.ValueStreamDescriptor.STREAM_CLASS_TYPE.OBSERVED, MyCaffe.basecode.descriptors.ValueStreamDescriptor.STREAM_VALUE_TYPE.NUMERIC, null, null, null, 1, null);
            int nStreamID_tickerid = db.AddValueStream(nSrcID, "Ticker ID", nOrdering++, ValueStreamDescriptor.STREAM_CLASS_TYPE.STATIC, ValueStreamDescriptor.STREAM_VALUE_TYPE.CATEGORICAL);

            RawValueDataCollection dataStatic = new RawValueDataCollection(null);
            dataStatic.Add(new RawValueData(ValueStreamDescriptor.STREAM_CLASS_TYPE.STATIC, ValueStreamDescriptor.STREAM_VALUE_TYPE.CATEGORICAL, nStreamID_tickerid));

            RawValueDataCollection data = new RawValueDataCollection(null);
            data.Add(new RawValueData(ValueStreamDescriptor.STREAM_CLASS_TYPE.TARGET, ValueStreamDescriptor.STREAM_VALUE_TYPE.NUMERIC, nStreamID_target_returns));
            data.Add(new RawValueData(ValueStreamDescriptor.STREAM_CLASS_TYPE.OBSERVED, ValueStreamDescriptor.STREAM_VALUE_TYPE.NUMERIC, nStreamID_norm_daily_returns));
            data.Add(new RawValueData(ValueStreamDescriptor.STREAM_CLASS_TYPE.OBSERVED, ValueStreamDescriptor.STREAM_VALUE_TYPE.NUMERIC, nStreamID_norm_monthly_returns));
            data.Add(new RawValueData(ValueStreamDescriptor.STREAM_CLASS_TYPE.OBSERVED, ValueStreamDescriptor.STREAM_VALUE_TYPE.NUMERIC, nStreamID_norm_quarterly_returns));
            data.Add(new RawValueData(ValueStreamDescriptor.STREAM_CLASS_TYPE.OBSERVED, ValueStreamDescriptor.STREAM_VALUE_TYPE.NUMERIC, nStreamID_norm_biannual_returns));
            data.Add(new RawValueData(ValueStreamDescriptor.STREAM_CLASS_TYPE.OBSERVED, ValueStreamDescriptor.STREAM_VALUE_TYPE.NUMERIC, nStreamID_norm_annual_returns));
            data.Add(new RawValueData(ValueStreamDescriptor.STREAM_CLASS_TYPE.OBSERVED, ValueStreamDescriptor.STREAM_VALUE_TYPE.NUMERIC, nStreamID_macd1));
            data.Add(new RawValueData(ValueStreamDescriptor.STREAM_CLASS_TYPE.OBSERVED, ValueStreamDescriptor.STREAM_VALUE_TYPE.NUMERIC, nStreamID_macd2));
            data.Add(new RawValueData(ValueStreamDescriptor.STREAM_CLASS_TYPE.OBSERVED, ValueStreamDescriptor.STREAM_VALUE_TYPE.NUMERIC, nStreamID_macd3));

            foreach (KeyValuePair<int, DataRecordCollection> kv in m_data.RecordsByCommodity)
            {
                kv.Value.PostProcess();
                if (kv.Value.Count < 120)
                    continue;

                int nCommodityID = kv.Key;
                string strCommodity = m_rgCommodities[nCommodityID];
                DateTime dtStart1 = kv.Value[0].Date;
                DateTime dtEnd1 = kv.Value[kv.Value.Count - 1].Date;
                int nSteps = kv.Value.Count;
                int nItemIdx = m_rgCommodityIDs[strCommodity];
                int nItemID = db.AddValueItem(nSrcID, nItemIdx, strCommodity, dtStart1, dtEnd1, nSteps);

                dataStatic.SetData(new float[] { nItemIdx });
                db.PutRawValue(nSrcID, nItemID, dataStatic);

                foreach (DataRecord rec in kv.Value.Items)
                {
                    if (!rec.IsValid)
                        continue;

                    DateTime dt = rec.Date;
                    List<float> rgfData = new List<float>();

                    rgfData.Add((float)rec.NormalizedItem(DataRecord.FIELD.TARGET_RETURNS));
                    rgfData.Add((float)rec.NormalizedItem(DataRecord.FIELD.NORM_DAILY_RETURNS));
                    rgfData.Add((float)rec.NormalizedItem(DataRecord.FIELD.NORM_MONTHLY_RETURNS));
                    rgfData.Add((float)rec.NormalizedItem(DataRecord.FIELD.NORM_QUARTERLY_RETURNS));
                    rgfData.Add((float)rec.NormalizedItem(DataRecord.FIELD.NORM_BIANNUAL_RETURNS));
                    rgfData.Add((float)rec.NormalizedItem(DataRecord.FIELD.NORM_ANNUAL_RETURNS));
                    rgfData.Add((float)rec.NormalizedItem(DataRecord.FIELD.MACD1));
                    rgfData.Add((float)rec.NormalizedItem(DataRecord.FIELD.MACD2));
                    rgfData.Add((float)rec.NormalizedItem(DataRecord.FIELD.MACD3));

                    data.SetData(dt, rgfData.ToArray());
                    db.PutRawValue(nSrcID, nItemID, data);

                    nIdx++;
                    nItemCount++;

                    if (nIdx % 3000 == 0)
                        db.SaveRawValues();

                    if (m_sw.Elapsed.TotalMilliseconds > 1000)
                    {
                        m_sw.Restart();

                        if (m_evtCancel.WaitOne(0))
                            break;

                        double dfPct = (double)nIdx / (double)nTotal;
                        m_log.WriteLine("Commodity = " + strCommodity + " - Saving '" + strSub + "' data to sql at " + dfPct.ToString("P") + " complete.");
                    }
                }

                db.SaveRawValues();

                if (m_evtCancel.WaitOne(0))
                    break;
            }

            db.UpdateSourceCounts(nItemCount);
            db.Close();

            return nSrcID;
        }
    }

    public class DataRecordCollection
    {
        string m_strTicker;
        bool m_bWinsorize = false;
        List<DataRecord> m_rgItems = new List<DataRecord>();
        ScalerCollection m_rgScalers = new ScalerCollection();
        double? m_dfLastDailyVol = null;

        public DataRecordCollection(string strTicker)
        {
            m_strTicker = strTicker;
        }

        public void GetDates(List<DateTime> rgDates)
        {
            foreach (DataRecord rec in m_rgItems)
            {
                if (!rgDates.Contains(rec.Date))
                    rgDates.Add(rec.Date);
            }
        }   

        public void PreProcess()
        {
            int VOL_THRESHOLD = 5;
            int HALFLIFE_WINSORIZE = 252;
            int VOL_LOOKBACK = 60;
            double VOL_TARGET = 0.15;
            CalculationArrayEx ca = new CalculationArrayEx(HALFLIFE_WINSORIZE + 1);
            CalculationArrayEx caVol = new CalculationArrayEx(VOL_LOOKBACK + 1);            
            MACD macd_8_24 = new MACD(8, 24);
            MACD macd_16_48 = new MACD(16, 48);
            MACD macd_32_96 = new MACD(32, 96);

            for (int i = 0; i < m_rgItems.Count-1; i++)
            {
                DataRecord rec = m_rgItems[i];
                DataRecord rec1 = m_rgItems[i + 1];

                if (ca.Add(rec.Srs, rec.Date, false))
                {
                    double dfSrs = rec.Srs;
                    double dfSrs1 = rec1.Srs;

                    if (m_bWinsorize)
                    {
                        double dfEwm = ca.AverageEwm;
                        double dfEwmStd = ca.StdDevEwm;

                        double dfMaxSrs = dfEwm + VOL_THRESHOLD * dfEwmStd;
                        double dfMinSrs = dfEwm - VOL_THRESHOLD * dfEwmStd;
                        dfSrs = Math.Min(dfSrs, dfMaxSrs);
                        dfSrs = Math.Max(dfSrs, dfMinSrs);
                    }

                    ca.Add(dfSrs, rec.Date, false);

                    double? dfDailyReturns = ca.CalculateReturns(1);

                    caVol.Add(dfDailyReturns.Value, rec.Date, false);
                    double? dfDailyVol = caVol.CalculateDailyVol(VOL_LOOKBACK);

                    if (m_dfLastDailyVol.HasValue && dfDailyVol.HasValue && (double.IsInfinity(dfDailyVol.Value) || double.IsNaN(dfDailyVol.Value) || dfDailyVol.Value == 0))
                        dfDailyVol = m_dfLastDailyVol.Value;

                    double? dfTargetReturns = (dfSrs1 - dfSrs) / dfSrs;

                    double? dfNormDailyReturns = ca.CalculateNormalizedReturns(1, dfDailyVol);
                    double? dfNormMonthlyReturns = ca.CalculateNormalizedReturns(21, dfDailyVol);
                    double? dfNormQuarterlyReturns = ca.CalculateNormalizedReturns(63, dfDailyVol);
                    double? dfNormBiannualReturns = ca.CalculateNormalizedReturns(126, dfDailyVol);
                    double? dfNormAnnualReturns = ca.CalculateNormalizedReturns(252, dfDailyVol);

                    double? dfMacd_8_24 = macd_8_24.Calculate(dfSrs, rec.Date);
                    double? dfMacd_16_48 = macd_16_48.Calculate(dfSrs, rec.Date);
                    double? dfMacd_32_96 = macd_32_96.Calculate(dfSrs, rec.Date);

                    if (dfNormAnnualReturns.HasValue && dfMacd_32_96.HasValue)
                    {
                        rec.Item(DataRecord.FIELD.TARGET_RETURNS, dfTargetReturns.Value);
                        rec.Item(DataRecord.FIELD.NORM_DAILY_RETURNS, dfNormDailyReturns.Value);
                        rec.Item(DataRecord.FIELD.NORM_MONTHLY_RETURNS, dfNormMonthlyReturns.Value);
                        rec.Item(DataRecord.FIELD.NORM_QUARTERLY_RETURNS, dfNormQuarterlyReturns.Value);
                        rec.Item(DataRecord.FIELD.NORM_BIANNUAL_RETURNS, dfNormBiannualReturns.Value);
                        rec.Item(DataRecord.FIELD.NORM_ANNUAL_RETURNS, dfNormAnnualReturns.Value);
                        rec.Item(DataRecord.FIELD.MACD1, dfMacd_8_24.Value);
                        rec.Item(DataRecord.FIELD.MACD2, dfMacd_16_48.Value);
                        rec.Item(DataRecord.FIELD.MACD3, dfMacd_32_96.Value);
                    }
                    else
                    {
                        rec.IsValid = false;
                    }

                    if (dfDailyVol.HasValue && !double.IsNaN(dfDailyVol.Value) && !double.IsInfinity(dfDailyVol.Value))
                        m_dfLastDailyVol = dfDailyVol.Value;
                }
                else
                {
                    rec.IsValid = false;
                }
            }
        }

        public bool Synchronize(List<DateTime> rgDateSync)
        {
            if (m_rgItems.Count == 0)
                return false;

            DateTime dt = m_rgItems[0].Date;

            int nSyncIdx = -1;
            for (int i=0; i<rgDateSync.Count; i++)
            {
                if (rgDateSync[i] == dt)
                {
                    nSyncIdx = i;
                    break;
                }
            }

            if (nSyncIdx == -1)
                return false;

            int nDataIdx = 0;

            while (nDataIdx < m_rgItems.Count && nSyncIdx < rgDateSync.Count)
            {
                if (m_rgItems[nDataIdx].Date == rgDateSync[nSyncIdx])
                {
                    nDataIdx++;
                    nSyncIdx++;
                }
                else if (m_rgItems[nDataIdx].Date > rgDateSync[nSyncIdx])
                {
                    DataRecord rec = m_rgItems[nDataIdx].Clone(rgDateSync[nSyncIdx]);
                    m_rgItems.Insert(nDataIdx, rec);
                    nSyncIdx++;
                    nDataIdx++;
                }
                else
                {
                    Trace.WriteLine("out of sync.");
                    return false;
                }
            }

            return true;
        }

        public void PostProcess()
        {
            m_rgItems = m_rgItems.Where(p => p.IsValid).OrderBy(p => p.Date).ToList();
        }

        public void SetScalers(int nLength = -1)
        {
            if (m_rgScalers.Count > 0)
                return;

            if (nLength == -1)
                nLength = Count;

            for (int i = 0; i < (int)DataRecord.FIELD.TOTAL; i++)
            {
                Scaler.SCALER scalerType = DataRecord.GetScalerType((DataRecord.FIELD)i);
                Scaler scaler = ScalerCollection.CreateScaler(scalerType, nLength);
                m_rgScalers.Add(i, scaler);
            }
        }

        public ScalerCollection Scalers
        {
            get { return m_rgScalers; }
            set { m_rgScalers = value; }
        }

        public void Add(DataRecord rec, bool bSetScaler)
        {
            m_rgItems.Add(rec);

            if (bSetScaler)
                rec.IsValid = m_rgScalers.Add(rec.Fields);

            if (rec.IsValid && m_rgScalers.Count > 0)
                m_rgScalers.Scale(rec.Fields, rec.NormalizedFields);
        }

        public DataRecord this[int nIdx]
        {
            get { return m_rgItems[nIdx]; }
        }

        public int Count
        {
            get { return m_rgItems.Count; }
        }

        public void SortByDate(DateTime dtEnd)
        {
            m_rgItems = m_rgItems.Where(p => p.Date <= dtEnd).OrderBy(p => p.Date).ToList();
        }

        public List<DataRecord> Items
        {
            get { return m_rgItems; }
        }
    }

    public class DataRecord
    {
        string m_strCommodity;
        bool m_bValid = true;
        int m_nCommodityID;
        DateTime m_dt;
        double m_dfSrs;
        double[] m_rgFields = new double[9];
        double[] m_rgFieldsNormalized = new double[9];

        static Scaler.SCALER[] m_rgScalerTypes = new Scaler.SCALER[9]
        {
            Scaler.SCALER.IDENTITY, // TARGET_RETURNS
            Scaler.SCALER.CENTER,   // NORM_DAILY_RETURNS
            Scaler.SCALER.CENTER,   // NORM_MONTHLY_RETURNS
            Scaler.SCALER.CENTER,   // NORM_QUARTERLY_RETURNS
            Scaler.SCALER.CENTER,   // NORM_BIANNUAL_RETURNS
            Scaler.SCALER.CENTER,   // NORM_ANNUAL_RETURNS
            Scaler.SCALER.CENTER,   // MACD1 
            Scaler.SCALER.CENTER,   // MACD2 
            Scaler.SCALER.CENTER,   // MACD2 
        };

        public enum FIELD
        {
            TARGET_RETURNS = 0,
            NORM_DAILY_RETURNS = 1,
            NORM_MONTHLY_RETURNS = 2,
            NORM_QUARTERLY_RETURNS = 3,
            NORM_BIANNUAL_RETURNS = 4,
            NORM_ANNUAL_RETURNS = 5,
            MACD1 = 6,
            MACD2 = 7,
            MACD3 = 8,

            TOTAL = 9
        }

        public DataRecord(int nCommodityID, DateTime dt, double dfSrs, string strCommodity)
        {
            m_strCommodity = strCommodity;
            m_nCommodityID = nCommodityID;
            m_dt = dt;
            m_dfSrs = dfSrs;
            m_rgFields[(int)FIELD.TARGET_RETURNS] = dfSrs;
        }

        public DataRecord(int nCommodityID, DateTime dt, double dfTarget, double dfNormDaily, double dfNormMonthly, double dfNormQuarterly, double dfNormBiannual, double dfNormAnnual, double dfMacd1, double dfMacd2, double dfMacd3)
        {
            m_nCommodityID = nCommodityID;
            m_dt = dt;
            m_rgFields[(int)FIELD.TARGET_RETURNS] = dfTarget;
            m_rgFields[(int)FIELD.NORM_DAILY_RETURNS] = dfNormDaily;
            m_rgFields[(int)FIELD.NORM_MONTHLY_RETURNS] = dfNormMonthly;
            m_rgFields[(int)FIELD.NORM_QUARTERLY_RETURNS] = dfNormQuarterly;
            m_rgFields[(int)FIELD.NORM_BIANNUAL_RETURNS] = dfNormBiannual;
            m_rgFields[(int)FIELD.NORM_ANNUAL_RETURNS] = dfNormAnnual;
            m_rgFields[(int)FIELD.MACD1] = dfMacd1;
            m_rgFields[(int)FIELD.MACD2] = dfMacd2;
            m_rgFields[(int)FIELD.MACD3] = dfMacd3;
        }

        public DataRecord Clone(DateTime dt)
        {
            DataRecord record = new DataRecord(
                m_nCommodityID,
                dt,
                m_dfSrs,
                m_strCommodity);
            record.m_bValid = m_bValid;
            return record;
        }

        public static Scaler.SCALER GetScalerType(FIELD field)
        {
            return m_rgScalerTypes[(int)field];
        }

        public string Symbol
        {
            get { return m_strCommodity; }
        }

        public bool IsValid
        {
            get { return m_bValid; }
            set { m_bValid = value; }
        }

        public int CommodityID
        {
            get { return m_nCommodityID; }
        }

        public DateTime Date
        {
            get { return m_dt; }
        }

        public double Srs
        {
            get { return m_dfSrs; }
        }

        public double Item(FIELD field)
        {
            return m_rgFields[(int)field];
        }

        public void Item(FIELD field, double dfVal)
        {
            m_rgFields[(int)field] = dfVal;
        }

        public double[] Fields
        {
            get { return m_rgFields; }
        }

        public double[] NormalizedFields
        {
            get { return m_rgFieldsNormalized; }
        }

        public double NormalizedItem(FIELD field)
        {
            return m_rgFieldsNormalized[(int)field];
        }

        public void NormalizedItem(FIELD field, double dfVal)
        {
            m_rgFieldsNormalized[(int)field] = dfVal;
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append(m_dt.ToString());
            sb.Append(' ');
            sb.Append(IsValid ? "VALID" : "INVALID");

            return sb.ToString();
        }
    }

    public class DataTable
    {
        int m_nDataWindow = 63;
        int m_nCount = 0;
        Dictionary<int, DataRecordCollection> m_rgRecordsByCommodity = new Dictionary<int, DataRecordCollection>();
        Dictionary<int, Tuple<int, int>> m_rgValidRangeByCommodity = new Dictionary<int, Tuple<int, int>>();
        DateTime m_dtStart = DateTime.MaxValue;

        public DataTable()
        {
        }

        public void PreProcess(DateTime dtStart, DateTime dtEnd, Log log, CancelEvent evtCancel, List<DateTime> rgDateSync)
        {
            List<int> rgDelete = new List<int>();          
            DateTime dtEndCutoff = dtEnd - TimeSpan.FromDays(2);

            foreach (KeyValuePair<int, DataRecordCollection> kv in m_rgRecordsByCommodity)
            {
                kv.Value.SortByDate(dtEnd + TimeSpan.FromDays(1));

                if (kv.Value.Count == 0)
                {
                    rgDelete.Add(kv.Key);
                    continue;
                }

                if (kv.Value.Items.First().Date > dtStart || kv.Value.Items.Last().Date < dtEndCutoff)
                {
                    rgDelete.Add(kv.Key);
                    continue;
                }
            }

            foreach (int nKey in rgDelete)
            {
                m_rgRecordsByCommodity.Remove(nKey);
            }

            rgDelete.Clear();
            
            Stopwatch sw = new Stopwatch();
            int nIdx = 0;

            sw.Start();
            foreach (KeyValuePair<int, DataRecordCollection> kv in m_rgRecordsByCommodity)
            {
                if (!kv.Value.Synchronize(rgDateSync))
                {
                    rgDelete.Add(kv.Key);
                    continue;
                }

                kv.Value.PreProcess();
                nIdx++;

                if (sw.Elapsed.TotalMilliseconds > 1000)
                {
                    sw.Restart();
                    double dfPct = (double)nIdx / (double)m_rgRecordsByCommodity.Count;
                    log.WriteLine("Preprocessing data at " + dfPct.ToString("P") + " complete.");

                    if (evtCancel.WaitOne(0))
                        break;                
                }
            }

            foreach (int nKey in rgDelete)
            {
                m_rgRecordsByCommodity.Remove(nKey);
            }
        }

        public DataTable Split(DateTime dtStart, DateTime dtEnd, DataTable dtScaler)
        {
            DataTable dt = new DataTable();
            foreach (KeyValuePair<int, DataRecordCollection> kv in m_rgRecordsByCommodity)
            {
                for (int i = 0; i < kv.Value.Count; i++)
                {
                    DataRecord rec = kv.Value[i];

                    if (rec.IsValid && rec.Date >= dtStart && rec.Date <= dtEnd)
                        dt.Add(rec, dtScaler);
                }
            }

            return dt;
        }

        public void Add(DataRecord rec)
        {
            if (rec.Date < m_dtStart)
                m_dtStart = rec.Date;

            if (!m_rgRecordsByCommodity.ContainsKey(rec.CommodityID))
            {
                m_rgRecordsByCommodity.Add(rec.CommodityID, new DataRecordCollection(rec.Symbol));
            }

            m_rgRecordsByCommodity[rec.CommodityID].Add(rec, false);
            m_nCount++;
        }

        public void Add(DataRecord rec, DataTable dt)
        {
            if (rec.Date < m_dtStart)
                m_dtStart = rec.Date;

            if (!m_rgRecordsByCommodity.ContainsKey(rec.CommodityID))
            {
                m_rgRecordsByCommodity.Add(rec.CommodityID, new DataRecordCollection(rec.Symbol));

                if (dt == null)
                    m_rgRecordsByCommodity[rec.CommodityID].SetScalers(m_nDataWindow);
                else
                    m_rgRecordsByCommodity[rec.CommodityID].Scalers = dt.m_rgRecordsByCommodity[rec.CommodityID].Scalers;
            }

            m_rgRecordsByCommodity[rec.CommodityID].Add(rec, dt == null);
            m_nCount++;
        }

        public int Columns
        {
            get { return m_rgRecordsByCommodity.First().Value.Count; }
        }

        public int Count
        {
            get { return m_nCount; }
        }

        public int RecordsPerCommodity
        {
            get { return m_rgRecordsByCommodity.First().Value.Count; }
        }

        public long[] TimeSync
        {
            get { return m_rgRecordsByCommodity.First().Value.Items.Select(p => ((DateTimeOffset)p.Date).ToUnixTimeSeconds()).ToArray(); }
        }

        public DateTime StartTime
        {
            get { return m_dtStart; }
            set { m_dtStart = value; }
        }

        public Dictionary<int, DataRecordCollection> RecordsByCommodity
        {
            get { return m_rgRecordsByCommodity; }
        }

        public Dictionary<int, Tuple<int, int>> ValidRangeByCommodity
        {
            get { return m_rgValidRangeByCommodity; }
        }

        public void Clear()
        {
            m_rgRecordsByCommodity.Clear();
        }
    }

    public class CalculationArrayEx : CalculationArray
    {
        double? m_dfLastVol = null;

        public CalculationArrayEx(int nMax) : base(nMax, true)
        {
        }

        public double? CalculateReturns(int nOffset, bool bEwm = true)
        {
            List<double> rg = (bEwm) ? m_rgdfEwm : m_rgdf;
            if (rg.Count < (nOffset + 1))
                return null;

            double dfLast = rg[rg.Count - 1];
            double dfFirst = rg[rg.Count - (nOffset + 1)];

            if (dfFirst == 0)
                return 0;

            return (dfLast / dfFirst) - 1.0;
        }

        public double? CalculateDailyVol(int nOffset, bool bEwm = true)
        {
            List<double> rg = (bEwm) ? m_rgdfEwm : m_rgdf;

            if (rg.Count < nOffset)
                return null;

            return CalculateStdDev(rg, nOffset, bEwm);
        }

        public double? CalculateVolScaledReturns(double dfVolTarget, double? dfDailyVol, bool bEwm = true)
        {
            if (!dfDailyVol.HasValue)
                return null;

            double dfAnnualVol = dfDailyVol.Value * Math.Sqrt(252);

            if (m_dfLastVol.HasValue)
            {
                double dfDailyReturns = LastVal;
                double dfScaled = dfDailyReturns * dfVolTarget;

                if (m_dfLastVol.Value != 0)
                    dfScaled /= m_dfLastVol.Value;

                m_dfLastVol = dfAnnualVol;
                return dfScaled;
            }
            else
            {
                m_dfLastVol = dfAnnualVol;
                return null;
            }
        }

        public double? CalculateNormalizedReturns(int nOffset, double? dfDailyVol, bool bEwm = true)
        {
            if (!dfDailyVol.HasValue)
                return null;

            double? dfReturns = CalculateReturns(nOffset, bEwm);
            if (!dfReturns.HasValue)
                return null;

            double dfNormReturns = dfReturns.Value;

            if (dfDailyVol.Value != 0)
                dfNormReturns /= dfDailyVol.Value;

            double dfAnnualized = dfNormReturns / Math.Sqrt(nOffset);

            return dfAnnualized;
        }
    }

    class MACD
    {
        CalculationArray m_caShort;
        CalculationArray m_caLong;
        CalculationArray m_caQuarter;
        CalculationArray m_caAnnual;

        public MACD(int nShort, int nLong)
        {
            m_caShort = new CalculationArray(nShort, true);
            m_caLong = new CalculationArray(nLong, true);
            m_caQuarter = new CalculationArray(63, true);
            m_caAnnual = new CalculationArray(252, true);
        }

        public double? Calculate(double dfVal, DateTime dt)
        {
            if (!m_caShort.Add(dfVal, dt, false))
                return null;

            if (!m_caLong.Add(dfVal, dt, false))
                return null;

            if (!m_caQuarter.Add(dfVal, dt, false))
                return null;

            double dfEwmShort = m_caShort.AverageEwm;
            double dfEwmLong = m_caLong.AverageEwm;
            double dfMacd = dfEwmShort - dfEwmLong;
            double dfQ = dfMacd / m_caQuarter.StdDev;
            
            if (!m_caAnnual.Add(dfQ, dt, false))
                return null;

            return dfQ / m_caAnnual.StdDev;            
        }
    }
}
