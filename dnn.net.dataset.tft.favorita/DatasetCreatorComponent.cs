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

/// WORK IN PROGRESS
namespace DNN.net.dataset.tft.favorita
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
            CSV,
            NPY
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
            get { return "TFT.Favorita"; }
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
            string strHolidaysDataFile = Properties.Settings.Default.HolidaysDataFile;
            string strItemsDataFile = Properties.Settings.Default.ItemsDataFile;
            string strOilDataFile = Properties.Settings.Default.OilDataFile;
            string strSampleSubmissionDataFile = Properties.Settings.Default.SampleSubmissionDataFile;
            string strStoresDataFile = Properties.Settings.Default.StoresDataFile;
            string strTestDataFile = Properties.Settings.Default.TestDataFile;
            string strTrainDataFile = Properties.Settings.Default.TrainDataFile;
            string strTransactionsDataFile = Properties.Settings.Default.TransactionsDataFile;
            string strOutputPath = Properties.Settings.Default.OutputPath;
            OUTPUT_TYPE outType = (OUTPUT_TYPE)Properties.Settings.Default.OutputType;
            DateTime dtStart = DateTime.Parse(Properties.Settings.Default.StartDate);
            DateTime dtEnd = DateTime.Parse(Properties.Settings.Default.EndDate);

            config.Settings.Add(new DataConfigSetting("Output Dataset Name", Name));
            config.Settings.Add(new DataConfigSetting("Holidays Data File", strHolidaysDataFile, DataConfigSetting.TYPE.FILENAME, "csv"));
            config.Settings.Add(new DataConfigSetting("Items Data File", strItemsDataFile, DataConfigSetting.TYPE.FILENAME, "csv"));
            config.Settings.Add(new DataConfigSetting("Oil Data File", strOilDataFile, DataConfigSetting.TYPE.FILENAME, "csv"));
            config.Settings.Add(new DataConfigSetting("Sample Submission Data File", strSampleSubmissionDataFile, DataConfigSetting.TYPE.FILENAME, "csv"));
            config.Settings.Add(new DataConfigSetting("Stores Data File", strStoresDataFile, DataConfigSetting.TYPE.FILENAME, "csv"));
            config.Settings.Add(new DataConfigSetting("Test Data File", strTestDataFile, DataConfigSetting.TYPE.FILENAME, "csv"));
            config.Settings.Add(new DataConfigSetting("Train Data File", strTrainDataFile, DataConfigSetting.TYPE.FILENAME, "csv"));
            config.Settings.Add(new DataConfigSetting("Transactions Data File", strTransactionsDataFile, DataConfigSetting.TYPE.FILENAME, "csv"));
            config.Settings.Add(new DataConfigSetting("Output Path", strOutputPath, DataConfigSetting.TYPE.DIRECTORY));
            config.Settings.Add(new DataConfigSetting("Start Date", dtStart, DataConfigSetting.TYPE.DATETIME));
            config.Settings.Add(new DataConfigSetting("End Date", dtEnd, DataConfigSetting.TYPE.DATETIME));
            addList(config, "Output Format", outType, OUTPUT_TYPE.CSV, OUTPUT_TYPE.NPY);
        }

        public void Create(DatasetConfiguration config, IXDatasetCreatorProgress progress)
        {
            string strHolidaysDataFile = Properties.Settings.Default.HolidaysDataFile;
            string strItemsDataFile = Properties.Settings.Default.ItemsDataFile;
            string strOilDataFile = Properties.Settings.Default.OilDataFile;
            string strSampleSubmissionDataFile = Properties.Settings.Default.SampleSubmissionDataFile;
            string strStoresDataFile = Properties.Settings.Default.StoresDataFile;
            string strTestDataFile = Properties.Settings.Default.TestDataFile;
            string strTrainDataFile = Properties.Settings.Default.TrainDataFile;
            string strTransactionsDataFile = Properties.Settings.Default.TransactionsDataFile;
            string strOutputPath = Properties.Settings.Default.OutputPath;
            DateTime dtStart = DateTime.Parse(Properties.Settings.Default.StartDate);
            DateTime dtEnd = DateTime.Parse(Properties.Settings.Default.EndDate);

            m_evtCancel.Reset();

            strHolidaysDataFile = config.Settings.Find("Holidays Data File").Value.ToString();
            strItemsDataFile = config.Settings.Find("Items Data File").Value.ToString();
            strOilDataFile = config.Settings.Find("Oil Data File").Value.ToString();
            strSampleSubmissionDataFile = config.Settings.Find("Sample Submission Data File").Value.ToString();
            strStoresDataFile = config.Settings.Find("Stores Data File").Value.ToString();
            strTestDataFile = config.Settings.Find("Test Data File").Value.ToString();
            strTrainDataFile = config.Settings.Find("Train Data File").Value.ToString();
            strTransactionsDataFile = config.Settings.Find("Transactions Data File").Value.ToString();
            strOutputPath = config.Settings.Find("Output Path").Value.ToString();
            dtStart = (DateTime)config.Settings.Find("Start Date").Value;
            dtEnd = (DateTime)config.Settings.Find("End Date").Value;

            DataConfigSetting ds = config.Settings.Find("Output Format");
            OptionItem opt = ds.Value as OptionItem;
            OUTPUT_TYPE outType = (OUTPUT_TYPE)opt.Index;

            m_iprogress = progress;

            Log log = new Log(Name + " Dataset Creator");
            log.OnWriteLine += new EventHandler<LogArg>(log_OnWriteLine);

            try
            {
                if (!Directory.Exists(strOutputPath))
                    throw new Exception("Could not find the output path '" + strOutputPath + "'.");

                Dictionary<string, string> rgFiles = new Dictionary<string, string>()
                {
                    { "Holidays", strHolidaysDataFile },
                    { "Items", strItemsDataFile },
                    { "Oil", strOilDataFile },
                    { "SampleSubmission", strSampleSubmissionDataFile },
                    { "Stores", strStoresDataFile },
                    { "Test", strTestDataFile },
                    { "Train", strTrainDataFile },
                    { "Transactions", strTransactionsDataFile }
                };

                foreach (KeyValuePair<string, string> kvp in rgFiles)
                {
                    if (!File.Exists(kvp.Value))
                        throw new Exception("Could not find the " + kvp.Key + " data file '" + kvp.Value + "'.");
                }

                log.WriteLine("Converting " + Name + " data files from " + dtStart.ToShortDateString() + " to " + dtEnd.ToShortDateString() + "...");

                FavoritaData data = new FavoritaData(rgFiles, log, m_evtCancel);

                if (data.LoadData(dtStart, dtEnd))
                {
                    if (outType == OUTPUT_TYPE.CSV)
                        data.SaveData(strOutputPath + "consolidated_favorita.csv");
                    else if (outType == OUTPUT_TYPE.NPY)
                        data.SaveAsNumpy(strOutputPath);
                    else
                        throw new Exception("Unknown output type '" + outType.ToString() + "'.");
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
                    log.WriteLine("Done converting VOC0712 data files.");
                    m_iprogress.OnCompleted(new CreateProgressArgs(1, "COMPLETED."));
                }

                Properties.Settings.Default.HolidaysDataFile = strHolidaysDataFile;
                Properties.Settings.Default.ItemsDataFile = strItemsDataFile;
                Properties.Settings.Default.OilDataFile = strOutputPath;
                Properties.Settings.Default.SampleSubmissionDataFile = strSampleSubmissionDataFile;
                Properties.Settings.Default.StoresDataFile = strStoresDataFile;
                Properties.Settings.Default.TestDataFile = strTestDataFile;
                Properties.Settings.Default.TrainDataFile = strTrainDataFile;
                Properties.Settings.Default.TransactionsDataFile = strTransactionsDataFile;
                Properties.Settings.Default.OilDataFile = strOilDataFile;
                Properties.Settings.Default.OutputPath = strOutputPath;
                Properties.Settings.Default.StartDate = dtStart.ToString();
                Properties.Settings.Default.EndDate = dtEnd.ToString();
                Properties.Settings.Default.OutputType = (int)outType;
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
