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
using System.Drawing;
using SimpleGraphing.GraphData;

namespace dnn.net.dataset.tft.commodity
{
    public class CommodityData
    {
        string m_strDataPath;
        string m_strDebugPath;
        bool m_bEnableDebugOutputTraining;
        bool m_bEnableDebugOutputTesting;
        bool m_bEnableExtendedData;
        DataTable m_data = new DataTable();
        Dictionary<int, string> m_rgCommodities = new Dictionary<int, string>();
        Dictionary<string, int> m_rgCommodityIDs = new Dictionary<string, int>();
        Log m_log;
        CancelEvent m_evtCancel;
        Stopwatch m_sw = new Stopwatch();
        DATASET_TYPE m_type = DATASET_TYPE.NONE;
        DataRecord.LOSS_TYPE m_lossType = DataRecord.LOSS_TYPE.SHARPE;

        public enum DATASET_TYPE
        {
            NONE,
            TRAIN,
            TEST
        }

        public CommodityData(string strDataPath, Log log, CancelEvent evtCancel, string strDebugPath, bool bEnableDebugOutputTraining, bool bEnableDebugOutputTesting, bool bEnableExtendedData, DataRecord.LOSS_TYPE lossType)
        {
            m_strDataPath = strDataPath;
            m_log = log;
            m_evtCancel = evtCancel;
            m_lossType = lossType;

            m_strDebugPath = strDebugPath;
            m_bEnableDebugOutputTesting = bEnableDebugOutputTesting;
            m_bEnableDebugOutputTraining = bEnableDebugOutputTraining;
            m_bEnableExtendedData = bEnableExtendedData;
        }

        public Tuple<CommodityData, CommodityData> SplitData(DateTime dtTrainingStart, DateTime dtTrainingEnd, DateTime dtTestingStart, DateTime dtTestingEnd)
        {
            CommodityData dataTrain = new CommodityData(null, m_log, m_evtCancel, m_strDebugPath, m_bEnableDebugOutputTraining, false, m_bEnableExtendedData, m_lossType);
            dataTrain.m_type = DATASET_TYPE.TRAIN;
            dataTrain.m_rgCommodities = m_rgCommodities;
            dataTrain.m_rgCommodityIDs = m_rgCommodityIDs;
            dataTrain.m_data = m_data.Split(dtTrainingStart, dtTrainingEnd, null);

            CommodityData dataTest = new CommodityData(null, m_log, m_evtCancel, m_strDebugPath, false, m_bEnableDebugOutputTesting, m_bEnableExtendedData, m_lossType);
            dataTest.m_type = DATASET_TYPE.TEST;
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

                        if (!rgDates.ContainsKey(dt))
                            rgDates.Add(dt, 0);

                        if (ca.IsFull)
                        {
                            double dfPctChange = (dfPrice - dfLastPrice) / dfLastPrice;

                            if (dfPrice == 0 || Math.Abs(dfPctChange) > 10)
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

                        DataRecord rec = new DataRecord(i, dt, dfPrice, strCommodity, m_bEnableExtendedData, m_lossType);

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

            m_log.WriteLine(m_data.RecordsByCommodity.Count.ToString() + " valid commodities were loaded.");

            return true;
        }

        public void DebugOutput(string strTag)
        {
            if (string.IsNullOrEmpty(m_strDebugPath))
                return;

            if (m_type == DATASET_TYPE.TRAIN && !m_bEnableDebugOutputTraining)
                return;

            if (m_type == DATASET_TYPE.TEST && !m_bEnableDebugOutputTesting)
                return;

            string strPath = m_strDebugPath.TrimEnd('\\') + "\\" + strTag;
            if (!Directory.Exists(strPath))
                Directory.CreateDirectory(strPath);

            foreach (KeyValuePair<int, DataRecordCollection> kv in m_data.RecordsByCommodity)
            {
                debug(strPath, kv.Value, "raw", strTag, false);
                debug(strPath, kv.Value, "norm", strTag, true);
            }
        }

        private void debug(string strPath, DataRecordCollection col, string strOrder, string strTag, bool bNormalized)
        {
            Tuple<string, Image>[] rgImg = col.Render(bNormalized, DataRecord.FIELD.TARGET_RETURNS, DataRecord.FIELD.NORM_DAILY_RETURNS, DataRecord.FIELD.NORM_MONTHLY_RETURNS, DataRecord.FIELD.NORM_QUARTERLY_RETURNS, DataRecord.FIELD.NORM_BIANNUAL_RETURNS, DataRecord.FIELD.NORM_ANNUAL_RETURNS, DataRecord.FIELD.MACD1, DataRecord.FIELD.MACD2, DataRecord.FIELD.MACD3);

            foreach (Tuple<string, Image> img in rgImg)
            {
                string strFile = Path.Combine(strPath, col.Symbol + "." + strOrder + "." + img.Item1 + "." + strTag + ".png");
                img.Item2.Save(strFile);
            }
        }

        public int SaveAsSql(string strName, string strSub)
        {
            DatabaseTemporal db = new DatabaseTemporal();

            m_sw.Start();

            int nIdx = 0;
            int nTotal = m_data.RecordsByCommodity.Sum(p => p.Value.Items.Count);
            int nWid = (m_bEnableExtendedData) ? 14 : 8;

            if (m_lossType == DataRecord.LOSS_TYPE.QUANTILE)
                nWid += 3;

            int nSrcID = db.AddSource(strName + "." + strSub, m_rgCommodities.Count, nWid, m_data.RecordsPerCommodity, true, 0, false);
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
            int nStreamID_macd4 = (!m_bEnableExtendedData) ? 0 : db.AddValueStream(nSrcID, "macd_3_6", nOrdering++, MyCaffe.basecode.descriptors.ValueStreamDescriptor.STREAM_CLASS_TYPE.OBSERVED, MyCaffe.basecode.descriptors.ValueStreamDescriptor.STREAM_VALUE_TYPE.NUMERIC, null, null, null, 1, null);
            int nStreamID_macd5 = (!m_bEnableExtendedData) ? 0 : db.AddValueStream(nSrcID, "macd_4_8", nOrdering++, MyCaffe.basecode.descriptors.ValueStreamDescriptor.STREAM_CLASS_TYPE.OBSERVED, MyCaffe.basecode.descriptors.ValueStreamDescriptor.STREAM_VALUE_TYPE.NUMERIC, null, null, null, 1, null);
            int nStreamID_macd6 = (!m_bEnableExtendedData) ? 0 : db.AddValueStream(nSrcID, "macd_6_12", nOrdering++, MyCaffe.basecode.descriptors.ValueStreamDescriptor.STREAM_CLASS_TYPE.OBSERVED, MyCaffe.basecode.descriptors.ValueStreamDescriptor.STREAM_VALUE_TYPE.NUMERIC, null, null, null, 1, null);
            int nStreamID_rsi1 = (!m_bEnableExtendedData) ? 0 : db.AddValueStream(nSrcID, "rsi_4", nOrdering++, MyCaffe.basecode.descriptors.ValueStreamDescriptor.STREAM_CLASS_TYPE.OBSERVED, MyCaffe.basecode.descriptors.ValueStreamDescriptor.STREAM_VALUE_TYPE.NUMERIC, null, null, null, 1, null);
            int nStreamID_rsi2 = (!m_bEnableExtendedData) ? 0 : db.AddValueStream(nSrcID, "rsi_6", nOrdering++, MyCaffe.basecode.descriptors.ValueStreamDescriptor.STREAM_CLASS_TYPE.OBSERVED, MyCaffe.basecode.descriptors.ValueStreamDescriptor.STREAM_VALUE_TYPE.NUMERIC, null, null, null, 1, null);
            int nStreamID_rsi3 = (!m_bEnableExtendedData) ? 0 : db.AddValueStream(nSrcID, "rsi_14", nOrdering++, MyCaffe.basecode.descriptors.ValueStreamDescriptor.STREAM_CLASS_TYPE.OBSERVED, MyCaffe.basecode.descriptors.ValueStreamDescriptor.STREAM_VALUE_TYPE.NUMERIC, null, null, null, 1, null);
            int nStreamID_tickerid = db.AddValueStream(nSrcID, "Ticker ID", nOrdering++, ValueStreamDescriptor.STREAM_CLASS_TYPE.STATIC, ValueStreamDescriptor.STREAM_VALUE_TYPE.CATEGORICAL);

            int nStreamID_dayofweek = (m_lossType == DataRecord.LOSS_TYPE.SHARPE) ? 0 : db.AddValueStream(nSrcID, "day_of_week", nOrdering++, ValueStreamDescriptor.STREAM_CLASS_TYPE.KNOWN, ValueStreamDescriptor.STREAM_VALUE_TYPE.NUMERIC);
            int nStreamID_dayofmonth = (m_lossType == DataRecord.LOSS_TYPE.SHARPE) ? 0 : db.AddValueStream(nSrcID, "day_of_month", nOrdering++, ValueStreamDescriptor.STREAM_CLASS_TYPE.KNOWN, ValueStreamDescriptor.STREAM_VALUE_TYPE.NUMERIC);
            int nStreamID_monthofyear = (m_lossType == DataRecord.LOSS_TYPE.SHARPE) ? 0 : db.AddValueStream(nSrcID, "month_of_year", nOrdering++, ValueStreamDescriptor.STREAM_CLASS_TYPE.KNOWN, ValueStreamDescriptor.STREAM_VALUE_TYPE.NUMERIC);

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

            if (m_bEnableExtendedData)
            {
                data.Add(new RawValueData(ValueStreamDescriptor.STREAM_CLASS_TYPE.OBSERVED, ValueStreamDescriptor.STREAM_VALUE_TYPE.NUMERIC, nStreamID_macd4));
                data.Add(new RawValueData(ValueStreamDescriptor.STREAM_CLASS_TYPE.OBSERVED, ValueStreamDescriptor.STREAM_VALUE_TYPE.NUMERIC, nStreamID_macd5));
                data.Add(new RawValueData(ValueStreamDescriptor.STREAM_CLASS_TYPE.OBSERVED, ValueStreamDescriptor.STREAM_VALUE_TYPE.NUMERIC, nStreamID_macd6));
                data.Add(new RawValueData(ValueStreamDescriptor.STREAM_CLASS_TYPE.OBSERVED, ValueStreamDescriptor.STREAM_VALUE_TYPE.NUMERIC, nStreamID_rsi1));
                data.Add(new RawValueData(ValueStreamDescriptor.STREAM_CLASS_TYPE.OBSERVED, ValueStreamDescriptor.STREAM_VALUE_TYPE.NUMERIC, nStreamID_rsi2));
                data.Add(new RawValueData(ValueStreamDescriptor.STREAM_CLASS_TYPE.OBSERVED, ValueStreamDescriptor.STREAM_VALUE_TYPE.NUMERIC, nStreamID_rsi3));
            }

            if (m_lossType == DataRecord.LOSS_TYPE.QUANTILE)
            {
                data.Add(new RawValueData(ValueStreamDescriptor.STREAM_CLASS_TYPE.KNOWN, ValueStreamDescriptor.STREAM_VALUE_TYPE.NUMERIC, nStreamID_dayofweek));
                data.Add(new RawValueData(ValueStreamDescriptor.STREAM_CLASS_TYPE.KNOWN, ValueStreamDescriptor.STREAM_VALUE_TYPE.NUMERIC, nStreamID_dayofmonth));
                data.Add(new RawValueData(ValueStreamDescriptor.STREAM_CLASS_TYPE.KNOWN, ValueStreamDescriptor.STREAM_VALUE_TYPE.NUMERIC, nStreamID_monthofyear));
            }

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

                    if (m_bEnableExtendedData)
                    {
                        rgfData.Add((float)rec.NormalizedItem(DataRecord.FIELD.MACD4));
                        rgfData.Add((float)rec.NormalizedItem(DataRecord.FIELD.MACD5));
                        rgfData.Add((float)rec.NormalizedItem(DataRecord.FIELD.MACD6));
                        rgfData.Add((float)rec.NormalizedItem(DataRecord.FIELD.RSI1));
                        rgfData.Add((float)rec.NormalizedItem(DataRecord.FIELD.RSI2));
                        rgfData.Add((float)rec.NormalizedItem(DataRecord.FIELD.RSI3));
                    }

                    if (m_lossType == DataRecord.LOSS_TYPE.QUANTILE)
                    {
                        rgfData.Add((float)rec.NormalizedItem(DataRecord.FIELD.DAY_OF_WEEK));
                        rgfData.Add((float)rec.NormalizedItem(DataRecord.FIELD.DAY_OF_MONTH));
                        rgfData.Add((float)rec.NormalizedItem(DataRecord.FIELD.MONTH_OF_YEAR));
                    }

                    for (int i=0; i<rgfData.Count; i++)
                    {
                        if (float.IsNaN(rgfData[i]) || float.IsInfinity(rgfData[i]))
                        {
                            m_log.WriteLine("WARNING: NaN or Infinity found in data, setting to 0.");
                            rgfData[i] = 0;
                        }
                    }

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

            m_log.WriteLine(DataRecord.ToOutputTypeString(), true);

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
        bool m_bMarkForDeletion = false;

        public DataRecordCollection(string strTicker)
        {
            m_strTicker = strTicker;
        }

        public string Symbol
        {
            get { return m_strTicker; }
        }

        public bool MarkForDeletion
        {
            get { return m_bMarkForDeletion; }
        }

        public void GetDates(List<DateTime> rgDates)
        {
            foreach (DataRecord rec in m_rgItems)
            {
                if (!rgDates.Contains(rec.Date))
                    rgDates.Add(rec.Date);
            }
        }

        public int GetMissingConsecutiveDates(DateTime dtStart, DateTime dtEnd, List<DateTime> rgDates, List<DateTime> rgMissingDates)
        {
            List<DateTime> rgDateLocal = m_rgItems.Select(p => p.Date).OrderBy(p => p).ToList();
            int nMaxConsecutive = 0;
            int nConsecutive = 0;

            for (int i=0; i<rgDates.Count; i++)
            {
                if (rgDates[i] < dtStart || rgDates[i] > dtEnd)
                    continue;

                if (!rgDateLocal.Contains(rgDates[i]))
                {
                    nConsecutive++;
                    nMaxConsecutive = Math.Max(nMaxConsecutive, nConsecutive);
                    rgMissingDates.Add(rgDates[i]);
                }
                else
                {
                    nConsecutive = 0;
                }
            }

            return nMaxConsecutive;
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
            MACD macd_3_6 = new MACD(3, 6);
            MACD macd_4_8 = new MACD(4, 8);
            MACD macd_6_12 = new MACD(6, 12);
            RSI rsi4 = new RSI(4);
            RSI rsi6 = new RSI(6);
            RSI rsi14 = new RSI(14);

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
                        ca.ReplaceLast(dfSrs);
                    }

                    double? dfDailyReturns = ca.CalculateReturns(1, false);

                    caVol.Add(dfDailyReturns.Value, rec.Date, false);
                    double? dfDailyVol = caVol.CalculateDailyVol(VOL_LOOKBACK);

                    if (m_dfLastDailyVol.HasValue && dfDailyVol.HasValue && (double.IsInfinity(dfDailyVol.Value) || double.IsNaN(dfDailyVol.Value) || dfDailyVol.Value == 0))
                        dfDailyVol = m_dfLastDailyVol.Value;

                    double? dfTargetReturns = (dfSrs1 - dfSrs) / dfSrs;

                    if (double.IsNaN(dfTargetReturns.Value) || double.IsInfinity(dfTargetReturns.Value))
                        Trace.WriteLine("here.");

                    double? dfNormDailyReturns = ca.CalculateNormalizedReturns(1, dfDailyVol);
                    double? dfNormMonthlyReturns = ca.CalculateNormalizedReturns(21, dfDailyVol);
                    double? dfNormQuarterlyReturns = ca.CalculateNormalizedReturns(63, dfDailyVol);
                    double? dfNormBiannualReturns = ca.CalculateNormalizedReturns(126, dfDailyVol);
                    double? dfNormAnnualReturns = ca.CalculateNormalizedReturns(252, dfDailyVol);

                    double? dfMacd_3_6 = macd_3_6.Calculate(dfSrs, rec.Date, ref m_bMarkForDeletion);
                    double? dfMacd_4_8 = macd_4_8.Calculate(dfSrs, rec.Date, ref m_bMarkForDeletion);
                    double? dfMacd_6_12 = macd_6_12.Calculate(dfSrs, rec.Date, ref m_bMarkForDeletion);
                    double? dfMacd_8_24 = macd_8_24.Calculate(dfSrs, rec.Date, ref m_bMarkForDeletion);
                    double? dfMacd_16_48 = macd_16_48.Calculate(dfSrs, rec.Date, ref m_bMarkForDeletion);
                    double? dfMacd_32_96 = macd_32_96.Calculate(dfSrs, rec.Date, ref m_bMarkForDeletion);
                    double? dfRsi_4 = rsi4.Calculate(dfSrs, rec.Date);
                    double? dfRsi_6 = rsi6.Calculate(dfSrs, rec.Date);
                    double? dfRsi_14 = rsi14.Calculate(dfSrs, rec.Date);

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
                        rec.Item(DataRecord.FIELD.MACD4, dfMacd_3_6.Value);
                        rec.Item(DataRecord.FIELD.MACD5, dfMacd_4_8.Value);
                        rec.Item(DataRecord.FIELD.MACD6, dfMacd_6_12.Value);
                        rec.Item(DataRecord.FIELD.RSI1, dfRsi_4.Value);
                        rec.Item(DataRecord.FIELD.RSI2, dfRsi_6.Value);
                        rec.Item(DataRecord.FIELD.RSI3, dfRsi_14.Value);
                        rec.Item(DataRecord.FIELD.DAY_OF_WEEK, (int)rec.Date.DayOfWeek);
                        rec.Item(DataRecord.FIELD.DAY_OF_MONTH, rec.Date.Day);
                        rec.Item(DataRecord.FIELD.MONTH_OF_YEAR, rec.Date.Month);
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

            for (int i = 0; i < DataRecord.FieldTotal; i++)
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
                rec.IsValid = m_rgScalers.Add(rec.Fields, DataRecord.FieldTotal);

            if (rec.IsValid && m_rgScalers.Count > 0)
                m_rgScalers.Scale(rec.Fields, rec.NormalizedFields, DataRecord.FieldTotal);
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

        public Tuple<string, Image>[] Render(bool bNormalized, params DataRecord.FIELD[] rgField)
        {
            List<Tuple<string, Image>> rgImg = new List<Tuple<string, Image>>();

            for (int j = 0; j < rgField.Count(); j++)
            {
                DataRecord.FIELD field = rgField[j];
                PlotCollection plots = new PlotCollection(field.ToString());

                for (int i = 0; i < m_rgItems.Count; i++)
                {
                    DataRecord rec = m_rgItems[i];

                    double dfVal = (bNormalized) ? rec.NormalizedItem(field) : rec.Item(field);
                    bool bActive = rec.IsValid;

                    if (double.IsNaN(dfVal) || double.IsInfinity(dfVal))
                        bActive = false;

                    Plot plot = new Plot(rec.Date.ToFileTime(), dfVal, null, bActive);
                    plot.Tag = rec.Date;
                    plots.Add(plot);
                }

                Image img = SimpleGraphingControl.QuickRender(plots, 2000, 600, false, ConfigurationAxis.VALUE_RESOLUTION.DAY_MONTH, null, true, null, true);
                rgImg.Add(new Tuple<string, Image>(field.ToString(), img));
            }

            return rgImg.ToArray();
        }
    }

    public class DataRecord
    {
        string m_strCommodity;
        bool m_bValid = true;
        int m_nCommodityID;
        DateTime m_dt;
        double m_dfSrs;
        double[] m_rgFields = new double[18];
        double[] m_rgFieldsNormalized = new double[18];
        static bool m_bExtendedData = false;
        static LOSS_TYPE m_lossType = LOSS_TYPE.SHARPE;
        static int m_nFieldTotal = 9;

        public enum LOSS_TYPE
        {
            SHARPE = 0,
            QUANTILE = 1
        }

        static Scaler.SCALER[] m_rgScalerTypes = new Scaler.SCALER[18]
        {
            Scaler.SCALER.IDENTITY, // TARGET_RETURNS
            Scaler.SCALER.IDENTITY, // NORM_DAILY_RETURNS
            Scaler.SCALER.IDENTITY, // NORM_MONTHLY_RETURNS
            Scaler.SCALER.IDENTITY, // NORM_QUARTERLY_RETURNS
            Scaler.SCALER.IDENTITY, // NORM_BIANNUAL_RETURNS
            Scaler.SCALER.IDENTITY, // NORM_ANNUAL_RETURNS
            Scaler.SCALER.IDENTITY, // MACD1 
            Scaler.SCALER.IDENTITY, // MACD2 
            Scaler.SCALER.IDENTITY, // MACD3 
            Scaler.SCALER.IDENTITY, // MACD4 
            Scaler.SCALER.IDENTITY, // MACD5 
            Scaler.SCALER.IDENTITY, // MACD6 
            Scaler.SCALER.IDENTITY, // RSI1
            Scaler.SCALER.IDENTITY, // RSI2
            Scaler.SCALER.IDENTITY, // RSI3
            Scaler.SCALER.CENTER,   // DAY_OF_WEEK 
            Scaler.SCALER.CENTER,   // DAY_OF_MONTH
            Scaler.SCALER.CENTER,   // MONTH_OF_YEAR
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
            MACD4 = 9,
            MACD5 = 10,
            MACD6 = 11,
            RSI1 = 12,
            RSI2 = 13,
            RSI3 = 14,
            DAY_OF_WEEK = 15,
            DAY_OF_MONTH = 16,
            MONTH_OF_YEAR = 17
        }

        public DataRecord(int nCommodityID, DateTime dt, double dfSrs, string strCommodity, bool bExtendedData, LOSS_TYPE lossType)
        {
            m_strCommodity = strCommodity;
            m_nCommodityID = nCommodityID;
            m_dt = dt;
            m_dfSrs = dfSrs;
            m_rgFields[(int)FIELD.TARGET_RETURNS] = dfSrs;
            m_lossType = lossType;
            m_bExtendedData = bExtendedData;

            m_nFieldTotal = 9;

            if (bExtendedData)
                m_nFieldTotal += 6;

            if (lossType == LOSS_TYPE.QUANTILE)
                m_nFieldTotal += 3;
        }

        public DataRecord(int nCommodityID, DateTime dt, double dfTarget, double dfNormDaily, double dfNormMonthly, double dfNormQuarterly, double dfNormBiannual, double dfNormAnnual, double dfMacd1, double dfMacd2, double dfMacd3, bool bExtendedData, LOSS_TYPE lossType)
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
            m_rgFields[(int)FIELD.DAY_OF_WEEK] = (int)dt.DayOfWeek;
            m_rgFields[(int)FIELD.DAY_OF_MONTH] = dt.Day;
            m_rgFields[(int)FIELD.MONTH_OF_YEAR] = dt.Month;

            m_lossType = lossType;
            m_bExtendedData = bExtendedData;

            m_nFieldTotal = 9;

            if (bExtendedData)
                m_nFieldTotal += 6;

            if (lossType == LOSS_TYPE.QUANTILE)
                m_nFieldTotal += 3;
        }

        public static string ToOutputTypeString()
        {
            string str = "";

            str = "--STATIC--" + Environment.NewLine;
            str += "0.) Ticker ID" + Environment.NewLine + Environment.NewLine;

            str += "--OBSERVED--" + Environment.NewLine;

            int nIdx = 0;
            for (int i=(int)FIELD.NORM_DAILY_RETURNS; i<= (int)FIELD.MACD3; i++)
            {
                str += nIdx.ToString() + ".) " + ((FIELD)i).ToString() + Environment.NewLine;
                nIdx++;
            }

            if (m_bExtendedData)
            {
                for (int i = (int)FIELD.MACD4; i <= (int)FIELD.RSI3; i++)
                {
                    str += nIdx.ToString() + ".) " + ((FIELD)i).ToString() + Environment.NewLine;
                    nIdx++;
                }
            }

            if (m_lossType == LOSS_TYPE.QUANTILE)
            {
                str += "--KNOWN--" + Environment.NewLine;
                nIdx = 0;
                for (int i = (int)FIELD.DAY_OF_WEEK; i <= (int)FIELD.MONTH_OF_YEAR; i++)
                {
                    str += nIdx.ToString() + ".) " + ((FIELD)i).ToString() + Environment.NewLine;
                    nIdx++;
                }
            }

            return str;
        }

        public DataRecord Clone(DateTime dt)
        {
            DataRecord record = new DataRecord(
                m_nCommodityID,
                dt,
                m_dfSrs,
                m_strCommodity,
                m_bExtendedData,
                m_lossType);
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

        public LOSS_TYPE LossType
        {
            get { return m_lossType; }
        }

        public static int FieldTotal
        {
            get { return m_nFieldTotal; }
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
            sb.Append(' ');
            sb.Append(m_dfSrs.ToString("N2"));

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
            DateTime dtMax = DateTime.MinValue;
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

                DateTime dtFirst = kv.Value.Items.First().Date;
                DateTime dtLast = kv.Value.Items.Last().Date;

                if (dtMax < dtLast)
                    dtMax = dtLast;

                if (dtFirst > dtStart || dtLast < dtEndCutoff)
                {
                    rgDelete.Add(kv.Key);
                    continue;
                }

                List<DateTime> rgMissingDates = new List<DateTime>();
                int nMissingCount = kv.Value.GetMissingConsecutiveDates(dtStart, dtEnd, rgDateSync, rgMissingDates);
                if (nMissingCount > 5)
                    rgDelete.Add(kv.Key);
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
                if (kv.Value.MarkForDeletion)
                    rgDelete.Add(kv.Key);
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

            log.WriteLine("Preprocessing completed - " + m_rgRecordsByCommodity.Count.ToString() + " commodities remain.", true);
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

        public double? Calculate(double dfVal, DateTime dt, ref bool bInvalid)
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

            double dfMacdFinal = dfQ / m_caAnnual.StdDev;

            if (double.IsNaN(dfMacdFinal) || double.IsInfinity(dfMacdFinal))
                bInvalid = true;

            return dfMacdFinal;
        }
    }

    class RSI
    {
        int m_nInterval = 0;
        GraphDataRSI m_rsi;
        RsiData m_rsiData;
        PlotCollection m_plots = new PlotCollection();
        PlotCollectionSet m_set = new PlotCollectionSet();

        public RSI(int nInterval)
        {
            ConfigurationPlot cfg = new ConfigurationPlot();
            cfg.PlotType = ConfigurationPlot.PLOTTYPE.RSI;
            cfg.Interval = (uint)nInterval;
            m_rsi = new GraphDataRSI(cfg);
            m_nInterval = nInterval;

            m_set.Add(m_plots);
            m_rsiData = m_rsi.Pre(m_set, 0);
        }

        public double? Calculate(double dfVal, DateTime dt)
        {
            double dfLast = 0;
            if (m_plots.Count > 0)
                dfLast = m_plots[m_plots.Count - 1].Y;  

            Plot plot = new Plot(dt.ToFileTime(), dfLast + dfVal);
            plot.Tag = dt;

            m_plots.Add(plot);

            bool bActive;
            double dfRsi = m_rsi.Process(m_rsiData, m_plots.Count - 1, out bActive, null, 0, false, true);

            if (!bActive)
                return null;

            return dfRsi / 100.0;
        }
    }
}
