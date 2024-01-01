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

            config.Settings.Add(new DataConfigSetting("Output Dataset Name", Name));
            config.Settings.Add(new DataConfigSetting("Data Path", strDataPath, DataConfigSetting.TYPE.DIRECTORY));

            config.Settings.Add(new DataConfigSetting("Train Start Date", Properties.Settings.Default.TrainStartDate, DataConfigSetting.TYPE.DATETIME));
            config.Settings.Add(new DataConfigSetting("Train End Date", Properties.Settings.Default.TrainEndDate, DataConfigSetting.TYPE.DATETIME));
            config.Settings.Add(new DataConfigSetting("Test Start Date", Properties.Settings.Default.TestStartDate, DataConfigSetting.TYPE.DATETIME));
            config.Settings.Add(new DataConfigSetting("Test End Date", Properties.Settings.Default.TestEndDate, DataConfigSetting.TYPE.DATETIME));
            addList(config, "Output Format", OUTPUT_TYPE.SQL, OUTPUT_TYPE.SQL);
        }

        public void Create(DatasetConfiguration config, IXDatasetCreatorProgress progress)
        {
            DateTime dtTrainStart = Properties.Settings.Default.TrainStartDate;
            DateTime dtTrainEnd = Properties.Settings.Default.TrainEndDate;
            DateTime dtTestStart = Properties.Settings.Default.TestStartDate;
            DateTime dtTestEnd = Properties.Settings.Default.TestEndDate;
            string strDataPath = Properties.Settings.Default.DataPath;

            m_evtCancel.Reset();
            m_iprogress = progress;

            Log log = new Log(Name + " Dataset Creator");
            log.OnWriteLine += new EventHandler<LogArg>(log_OnWriteLine);

            try
            {
                DataConfigSetting dataPath = config.Settings.Find("Data Path");
                if (dataPath != null)
                    strDataPath = dataPath.Value.ToString();

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

                Properties.Settings.Default.DataPath = strDataPath;
                Properties.Settings.Default.TrainStartDate = dtTrainStart;
                Properties.Settings.Default.TrainEndDate = dtTrainEnd;
                Properties.Settings.Default.TestStartDate = dtTestStart;
                Properties.Settings.Default.TestEndDate = dtTestEnd;
                Properties.Settings.Default.Save();

                if (!Directory.Exists(strDataPath))
                    throw new Exception("Could not find the data path '" + strDataPath + "'.");

                CommodityData data = new CommodityData(strDataPath, log, m_evtCancel);
                if (data.LoadData(dtTestStart - TimeSpan.FromDays(365 * 3), dtTestEnd))
                {
                    Tuple<CommodityData, CommodityData> data1 = data.SplitData(dtTrainStart, dtTrainEnd, dtTestStart, dtTestEnd);
                    CommodityData dataTrain = data1.Item1;
                    CommodityData dataTest = data1.Item2;

                    DatabaseTemporal db = new DatabaseTemporal();
                    db.DeleteDataset(Name, false, log, m_evtCancel);
                    int nTrainSrcID = dataTrain.SaveAsSql(Name, "train");
                    int nTestSrcID = dataTest.SaveAsSql(Name, "test");
                    //dataVal.SaveAsSql(Name, "validation");
                    db.AddDataset(config.ID, Name, nTestSrcID, nTrainSrcID, 0, 0, null, false);
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
                    log.WriteLine("ABORTED converting " + Name + " data files.");
                    m_iprogress.OnCompleted(new CreateProgressArgs(1, "ABORTED!", null, true));
                }
                else
                {
                    log.WriteLine("Done loading and processing Commodity data files.");
                    m_iprogress.OnCompleted(new CreateProgressArgs(1, "COMPLETED."));
                }

                Properties.Settings.Default.DataPath = strDataPath;
                Properties.Settings.Default.TrainStartDate = dtTrainStart;
                Properties.Settings.Default.TrainEndDate = dtTrainEnd;
                Properties.Settings.Default.TestStartDate = dtTestStart;
                Properties.Settings.Default.TestEndDate = dtTestEnd;
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
