using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Text;
using MyCaffe.db.image;
using MyCaffe.basecode;
using DNN.net.dataset.common;
using System.IO;
using MyCaffe.data;
using System.Threading;
using MyCaffe.db.temporal;
using dnn.net.dataset.tft.commodity;

namespace dnn.net.dataset.tft.commodities
{
    public partial class DatasetCreatorComponent : Component, IXDatasetCreator
    {
        IXDatasetCreatorProgress m_iprogress = null;
        DatasetFactory m_factory = new DatasetFactory();
        CancelEvent m_evtCancel = new CancelEvent();
        DB_VERSION m_dbVer = DB_VERSION.DEFAULT;

        public enum BOOLEAN
        {
            False = 0,
            True = 1
        }

        public enum OUTPUT_TYPE
        {
            SQL = 1
        }

        public DatasetCreatorComponent()
        {
            InitializeComponent();
        }

        public DatasetCreatorComponent(IContainer container)
        {
            container.Add(this);

            InitializeComponent();
        }

        public string Name
        {
            get { return "TFT.commodities"; }
        }

        public DB_VERSION DbVersion
        {
            get { return m_dbVer; }
            set { m_dbVer = value; }
        }

        private void addList(DatasetConfiguration config, string strName, object objDefault, params object[] rgParam)
        {
            OptionItemList rgItems = new OptionItemList();

            foreach (object obj in rgParam)
            {
                rgItems.Add(new OptionItem(obj.ToString(), (int)obj, null));
            }

            OptionItem item = new OptionItem(objDefault.ToString(), (int)objDefault, rgItems);
            config.Settings.Add(new DataConfigSetting(strName, item, DataConfigSetting.TYPE.LIST));
        }

        public void QueryConfiguration(DatasetConfiguration config)
        {
            string strDataPath = Properties.Settings.Default.DataPath;
            string strDebugFolder = Properties.Settings.Default.DebugFolder;

            config.Settings.Add(new DataConfigSetting("Output Dataset Name", Name));
            config.Settings.Add(new DataConfigSetting("Data Path", strDataPath, DataConfigSetting.TYPE.DIRECTORY));
            config.Settings.Add(new DataConfigSetting("Debug Path", strDebugFolder, DataConfigSetting.TYPE.DIRECTORY));

            config.Settings.Add(new DataConfigSetting("Train Start Date", Properties.Settings.Default.TrainStartDate, DataConfigSetting.TYPE.DATETIME));
            config.Settings.Add(new DataConfigSetting("Train End Date", Properties.Settings.Default.TrainEndDate, DataConfigSetting.TYPE.DATETIME));
            config.Settings.Add(new DataConfigSetting("Test Start Date", Properties.Settings.Default.TestStartDate, DataConfigSetting.TYPE.DATETIME));
            config.Settings.Add(new DataConfigSetting("Test End Date", Properties.Settings.Default.TestEndDate, DataConfigSetting.TYPE.DATETIME));
            addList(config, "Output Format", OUTPUT_TYPE.SQL, OUTPUT_TYPE.SQL);
            addList(config, "Loss Type", (DataRecord.LOSS_TYPE)Properties.Settings.Default.LossType, DataRecord.LOSS_TYPE.SHARPE, DataRecord.LOSS_TYPE.QUANTILE);

            addList(config, "Enable Debug Output Training", (Properties.Settings.Default.EnableDebugOutputTraining) ? BOOLEAN.True : BOOLEAN.False, BOOLEAN.False, BOOLEAN.True);
            addList(config, "Enable Debug Output Testing", (Properties.Settings.Default.EnableDebugOutputTesting) ? BOOLEAN.True : BOOLEAN.False, BOOLEAN.False, BOOLEAN.True);
            addList(config, "Enable Extended Data", (Properties.Settings.Default.EnableExtendedData) ? BOOLEAN.True : BOOLEAN.False, BOOLEAN.False, BOOLEAN.True);
        }

        public void Create(DatasetConfiguration config, IXDatasetCreatorProgress progress)
        {
            DateTime dtTrainStart = Properties.Settings.Default.TrainStartDate;
            DateTime dtTrainEnd = Properties.Settings.Default.TrainEndDate;
            DateTime dtTestStart = Properties.Settings.Default.TestStartDate;
            DateTime dtTestEnd = Properties.Settings.Default.TestEndDate;
            string strDataPath = Properties.Settings.Default.DataPath;
            string strDebugPath = Properties.Settings.Default.DebugFolder;
            bool bEnableDebugOutputTraining = Properties.Settings.Default.EnableDebugOutputTraining;
            bool bEnableDebugOutputTesting = Properties.Settings.Default.EnableDebugOutputTesting;
            bool bEnableExtendedData = Properties.Settings.Default.EnableExtendedData;
            DataRecord.LOSS_TYPE lossType = (DataRecord.LOSS_TYPE)Properties.Settings.Default.LossType;
            string strDatasetName = Name;

            m_evtCancel.Reset();
            m_iprogress = progress;

            Log log = new Log(Name + " Dataset Creator");
            log.OnWriteLine += new EventHandler<LogArg>(log_OnWriteLine);

            try
            {
                OptionItem item;
                DataConfigSetting dataPath = config.Settings.Find("Data Path");
                if (dataPath != null)
                    strDataPath = dataPath.Value.ToString();

                DataConfigSetting debugPath = config.Settings.Find("Debug Path");
                if (debugPath != null)
                    strDebugPath = debugPath.Value.ToString();

                DataConfigSetting datasetName = config.Settings.Find("Output Dataset Name");
                if (datasetName != null)
                    strDatasetName = datasetName.Value.ToString();

                DataConfigSetting trainStart = config.Settings.Find("Train Start Date");
                if (trainStart != null)
                    dtTrainStart = DateTime.Parse(trainStart.Value.ToString());

                DataConfigSetting trainEnd = config.Settings.Find("Train End Date");
                if (trainEnd != null)
                    dtTrainEnd = DateTime.Parse(trainEnd.Value.ToString());

                DataConfigSetting testStart = config.Settings.Find("Test Start Date");
                if (testStart != null)
                    dtTestStart = DateTime.Parse(testStart.Value.ToString());

                DataConfigSetting testEnd = config.Settings.Find("Test End Date");
                if (testEnd != null)
                    dtTestEnd = DateTime.Parse(testEnd.Value.ToString());

                DataConfigSetting lossType1 = config.Settings.Find("Loss Type");
                if (lossType1 != null)
                {
                    item = lossType1.Value as OptionItem;
                    lossType = (DataRecord.LOSS_TYPE)item.Index;
                }

                DataConfigSetting enableDebugTrain = config.Settings.Find("Enable Debug Output Training");
                if (enableDebugTrain != null)
                {
                    item = enableDebugTrain.Value as OptionItem;
                    bEnableDebugOutputTraining = item.Index == 1 ? true : false;
                }

                DataConfigSetting enableDebugTest = config.Settings.Find("Enable Debug Output Testing");
                if (enableDebugTest != null)
                {
                    item = enableDebugTest.Value as OptionItem;
                    bEnableDebugOutputTesting = item.Index == 1 ? true : false;
                }

                DataConfigSetting enableExtendedData = config.Settings.Find("Enable Extended Data");
                if (enableExtendedData != null)
                {
                    item = enableExtendedData.Value as OptionItem;
                    bEnableExtendedData = item.Index == 1 ? true : false;
                }

                Properties.Settings.Default.DataPath = strDataPath;
                Properties.Settings.Default.DebugFolder = strDebugPath;
                Properties.Settings.Default.TrainStartDate = dtTrainStart;
                Properties.Settings.Default.TrainEndDate = dtTrainEnd;
                Properties.Settings.Default.TestStartDate = dtTestStart;
                Properties.Settings.Default.TestEndDate = dtTestEnd;
                Properties.Settings.Default.EnableDebugOutputTraining = bEnableDebugOutputTraining;
                Properties.Settings.Default.EnableDebugOutputTesting = bEnableDebugOutputTesting;
                Properties.Settings.Default.LossType = (int)lossType;
                Properties.Settings.Default.EnableExtendedData = bEnableExtendedData;
                Properties.Settings.Default.Save();

                if (!Directory.Exists(strDataPath))
                    throw new Exception("Could not find the data path '" + strDataPath + "'.");

                CommodityData data = new CommodityData(strDataPath, log, m_evtCancel, strDebugPath, bEnableDebugOutputTraining, bEnableDebugOutputTesting, bEnableExtendedData, lossType);
                if (data.LoadData(dtTrainEnd - TimeSpan.FromDays(365), dtTestEnd))
                {
                    Tuple<CommodityData, CommodityData> data1 = data.SplitData(dtTrainStart, dtTrainEnd, dtTestStart, dtTestEnd);
                    CommodityData dataTrain = data1.Item1;
                    CommodityData dataTest = data1.Item2;

                    DatabaseTemporal db = new DatabaseTemporal();
                    db.DeleteDataset(strDatasetName, false, log, m_evtCancel);

                    dataTrain.DebugOutput("train");
                    dataTest.DebugOutput("test");

                    int nTrainSrcID = dataTrain.SaveAsSql(strDatasetName, "train");
                    int nTestSrcID = dataTest.SaveAsSql(strDatasetName, "test");
                    //dataVal.SaveAsSql(strDatasetName, "validation");
                    db.AddDataset(config.ID, strDatasetName, nTestSrcID, nTrainSrcID, 0, 0, null, false);
                }
            }
            catch (Exception excpt)
            {
                log.WriteLine("ERROR: " + excpt.Message);
            }
            finally
            {
                if (m_evtCancel.WaitOne(0))
                {
                    log.WriteLine("ABORTED converting " + strDatasetName + " data files.");
                    m_iprogress.OnCompleted(new CreateProgressArgs(1, "ABORTED!", null, true));
                }
                else
                {
                    log.WriteLine("Done loading and processing Commodity data files.");
                    m_iprogress.OnCompleted(new CreateProgressArgs(1, "COMPLETED."));
                }

                Properties.Settings.Default.DataPath = strDataPath;
                Properties.Settings.Default.DebugFolder = strDebugPath;
                Properties.Settings.Default.TrainStartDate = dtTrainStart;
                Properties.Settings.Default.TrainEndDate = dtTrainEnd;
                Properties.Settings.Default.TestStartDate = dtTestStart;
                Properties.Settings.Default.TestEndDate = dtTestEnd;
                Properties.Settings.Default.EnableDebugOutputTraining = bEnableDebugOutputTraining;
                Properties.Settings.Default.EnableDebugOutputTesting = bEnableDebugOutputTesting;
                Properties.Settings.Default.EnableExtendedData = bEnableExtendedData;
                Properties.Settings.Default.Save();
            }
        }

        private void Loader_OnError(object sender, ProgressArgs e)
        {
            m_iprogress.OnProgress(new CreateProgressArgs(e.Progress.Percentage, e.Progress.Message, e.Progress.Error));
        }

        private void Loader_OnProgress(object sender, ProgressArgs e)
        {
            m_iprogress.OnProgress(new CreateProgressArgs(e.Progress.Percentage, e.Progress.Message));
        }

        private void Loader_OnCompleted(object sender, EventArgs e)
        {
            m_iprogress.OnCompleted(new CreateProgressArgs(1, "DONE"));
        }

        void log_OnWriteLine(object sender, LogArg e)
        {
            if (m_iprogress != null)
            {
                CreateProgressArgs arg = new CreateProgressArgs(e.Progress, e.Message);
                m_iprogress.OnProgress(arg);

                if (arg.Abort)
                    m_evtCancel.Set();

                Thread.Sleep(0);
            }
        }
    }
}
