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

namespace DNN.net.dataset.tft.volatility
{
    public partial class DatasetCreatorComponent : Component, IXDatasetCreator
    {
        IXDatasetCreatorProgress m_iprogress = null;
        DatasetFactory m_factory = new DatasetFactory();
        CancelEvent m_evtCancel = new CancelEvent();
        IMGDB_VERSION m_imgDbVer = IMGDB_VERSION.DEFAULT;

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
            get { return "TFT.Volatility"; }
        }

        public IMGDB_VERSION ImageDbVersion
        {
            get { return m_imgDbVer; }
            set { m_imgDbVer = value; }
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
            string strDataFile = Properties.Settings.Default.DataFile;
            string strOutputPath = Properties.Settings.Default.OutputPath;
            OUTPUT_TYPE outType = (OUTPUT_TYPE)Properties.Settings.Default.OutputType;

            config.Settings.Add(new DataConfigSetting("Output Dataset Name", Name));
            config.Settings.Add(new DataConfigSetting("Data File", strDataFile, DataConfigSetting.TYPE.FILENAME, "csv"));
            config.Settings.Add(new DataConfigSetting("Output Path", strOutputPath, DataConfigSetting.TYPE.DIRECTORY));
            config.Settings.Add(new DataConfigSetting("Train Split", Properties.Settings.Default.TrainingSplitPct, DataConfigSetting.TYPE.REAL));
            config.Settings.Add(new DataConfigSetting("Test Split", Properties.Settings.Default.TestingSplitPct, DataConfigSetting.TYPE.REAL));
            config.Settings.Add(new DataConfigSetting("Validation Split", Properties.Settings.Default.ValidSplitPct, DataConfigSetting.TYPE.REAL));
            addList(config, "Output Format", outType, OUTPUT_TYPE.CSV, OUTPUT_TYPE.NPY);
        }

        public void Create(DatasetConfiguration config, IXDatasetCreatorProgress progress)
        {
            string strDataFile = Properties.Settings.Default.DataFile;
            string strOutputPath = Properties.Settings.Default.OutputPath;

            m_evtCancel.Reset();

            strDataFile = config.Settings.Find("Data File").Value.ToString();
            strOutputPath = config.Settings.Find("Output Path").Value.ToString();
            double dfTrainSplit = (double)config.Settings.Find("Train Split").Value;
            double dfTestSplit = (double)config.Settings.Find("Test Split").Value;
            double dfValSplit = (double)config.Settings.Find("Validation Split").Value;


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

                if (!File.Exists(strDataFile))
                    throw new Exception("Could not find the data file '" + strDataFile + "'.");

                VolatilityData data = new VolatilityData(strDataFile, log, m_evtCancel);

                DateTime dtStart = DateTime.MinValue;
                DateTime dtEnd = DateTime.MaxValue;

                if (data.LoadData(dtStart, dtEnd))
                {
                    VolatilityData dataTrain = data.SplitData(0, dfTrainSplit);
                    VolatilityData dataTest = data.SplitData(dfTrainSplit, dfTrainSplit + dfTestSplit);
                    VolatilityData dataVal = data.SplitData(dfTrainSplit + dfTestSplit, 1);

                    Dictionary<int, Dictionary<DataRecord.FIELD, Tuple<double, double>>> rgScalers = new Dictionary<int, Dictionary<DataRecord.FIELD, Tuple<double, double>>>();
                    dataTrain.NormalizeData(rgScalers);
                    dataTest.NormalizeData(rgScalers);
                    dataVal.NormalizeData(rgScalers);

                    if (outType == OUTPUT_TYPE.NPY)
                    {
                        dataTrain.SaveAsNumpy(strOutputPath, "train");
                        dataTest.SaveAsNumpy(strOutputPath, "test");
                        dataVal.SaveAsNumpy(strOutputPath, "validation");
                    }
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
                    log.WriteLine("Done converting " + Name + " Volatility data files.");
                    m_iprogress.OnCompleted(new CreateProgressArgs(1, "COMPLETED."));
                }

                Properties.Settings.Default.DataFile = strDataFile;
                Properties.Settings.Default.OutputPath = strOutputPath;
                Properties.Settings.Default.OutputType = (int)outType;
                Properties.Settings.Default.TrainingSplitPct = dfTrainSplit;
                Properties.Settings.Default.TestingSplitPct = dfTestSplit;
                Properties.Settings.Default.ValidSplitPct = dfValSplit;
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
