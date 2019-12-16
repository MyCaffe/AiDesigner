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

namespace DNN.net.dataset.voc0712
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
            get { return "VOC0712"; }
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
            string strTrainingFile1 = Properties.Settings.Default.vocTrainingFile1;
            string strTrainingFile2 = Properties.Settings.Default.vocTrainingFile2;
            string strTestingFile1 = Properties.Settings.Default.vocTestingFile1;
            bool bExpandFiles = Properties.Settings.Default.ExpandFiles;

            config.Settings.Add(new DataConfigSetting("Output Dataset Name", "VOC0712"));
            config.Settings.Add(new DataConfigSetting("Testing Data File 2007", strTestingFile1, DataConfigSetting.TYPE.FILENAME, "tar"));
            config.Settings.Add(new DataConfigSetting("Training Data File 2007", strTrainingFile1, DataConfigSetting.TYPE.FILENAME, "tar"));
            config.Settings.Add(new DataConfigSetting("Training Data File 2012", strTrainingFile2, DataConfigSetting.TYPE.FILENAME, "tar"));
            addList(config, "Extract Data Files", (bExpandFiles) ? BOOLEAN.True : BOOLEAN.False, BOOLEAN.True, BOOLEAN.False);
        }

        public void Create(DatasetConfiguration config, IXDatasetCreatorProgress progress)
        {
            string strTrainingFile1 = Properties.Settings.Default.vocTrainingFile1;
            string strTrainingFile2 = Properties.Settings.Default.vocTrainingFile2;
            string strTestingFile1 = Properties.Settings.Default.vocTestingFile1;
            bool bExpandFiles = Properties.Settings.Default.ExpandFiles;

            m_evtCancel.Reset();

            DataConfigSetting dsTrainingFile1 = config.Settings.Find("Training Data File 2007");
            strTrainingFile1 = dsTrainingFile1.Value.ToString();

            DataConfigSetting dsTrainingFile2 = config.Settings.Find("Training Data File 2012");
            strTrainingFile2 = dsTrainingFile2.Value.ToString();

            DataConfigSetting dsTestingFile1 = config.Settings.Find("Testing Data File 2007");
            strTestingFile1 = dsTestingFile1.Value.ToString();

            DataConfigSetting dsExtract = config.Settings.Find("Extract Data Files");
            OptionItem extractOption = dsExtract.Value as OptionItem;
            bool bExtract = (extractOption.Index == 0) ? false : true;

            DataConfigSetting dsName = config.Settings.Find("Output Dataset Name");
            string strDsName = dsName.Value.ToString();

            string strTrainingSrc = strDsName + ".training";
            string strTestingSrc = strDsName + ".testing";

            m_iprogress = progress;

            m_factory.DeleteSources(strTrainingSrc, strTestingSrc);

            Log log = new Log("VOC0712 Dataset Creator");
            log.OnWriteLine += new EventHandler<LogArg>(log_OnWriteLine);

            try
            {
                VOCDataParameters param = new VOCDataParameters(strTrainingFile1, strTrainingFile2, strTestingFile1, bExpandFiles);
                VOCDataLoader loader = new VOCDataLoader(param, log, m_evtCancel);

                loader.OnProgress += Loader_OnProgress;
                loader.OnError += Loader_OnError;
                loader.OnCompleted += Loader_OnCompleted;

                if (!loader.LoadDatabase(config.ID))
                    return;

                using (DNNEntities entities = EntitiesConnection.CreateEntities())
                {
                    List<Dataset> rgDs = entities.Datasets.Where(p => p.Name == strDsName).ToList();
                    List<Source> rgSrcTraining = entities.Sources.Where(p => p.Name == strTrainingSrc).ToList();
                    List<Source> rgSrcTesting = entities.Sources.Where(p => p.Name == strTestingSrc).ToList();

                    if (rgSrcTraining.Count == 0)
                        throw new Exception("Could not find the training source '" + strTrainingSrc + "'.");

                    if (rgSrcTesting.Count == 0)
                        throw new Exception("Could not find the tesing source '" + strTestingSrc + "'.");

                    int nSrcTestingCount = rgSrcTesting[0].ImageCount.GetValueOrDefault();
                    int nSrcTrainingCount = rgSrcTraining[0].ImageCount.GetValueOrDefault();
                    int nSrcTotalCount = nSrcTestingCount + nSrcTrainingCount;
                    double dfTestingPct = (nSrcTrainingCount == 0) ? 0.0 : nSrcTestingCount / (double)nSrcTotalCount;

                    Dataset ds = null;

                    if (rgDs.Count == 0)
                    {
                        ds = new Dataset();
                        ds.Name = strDsName;
                    }
                    else
                    {
                        ds = rgDs[0];
                    }
                    
                    ds.ImageEncoded = rgSrcTesting[0].ImageEncoded;
                    ds.ImageChannels = rgSrcTesting[0].ImageChannels;
                    ds.ImageHeight = rgSrcTraining[0].ImageHeight;
                    ds.ImageWidth = rgSrcTraining[0].ImageWidth;
                    ds.TestingPercent = (decimal)dfTestingPct;
                    ds.TestingSourceID = rgSrcTesting[0].ID;
                    ds.TestingTotal = rgSrcTesting[0].ImageCount;
                    ds.TrainingSourceID = rgSrcTraining[0].ID;
                    ds.TrainingTotal = rgSrcTraining[0].ImageCount;
                    ds.DatasetCreatorID = config.ID;
                    ds.DatasetGroupID = 0;
                    ds.ModelGroupID = 0;

                    if (rgDs.Count == 0)
                        entities.Datasets.Add(ds);

                    entities.SaveChanges();
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
                    log.WriteLine("ABORTED converting MNIST data files.");
                    m_iprogress.OnCompleted(new CreateProgressArgs(1, "ABORTED!", null, true));
                }
                else
                {
                    log.WriteLine("Done converting MNIST data files.");
                    m_iprogress.OnCompleted(new CreateProgressArgs(1, "COMPLETED."));
                }

                Properties.Settings.Default.vocTestingFile1 = strTestingFile1;
                Properties.Settings.Default.vocTrainingFile1 = strTrainingFile1;
                Properties.Settings.Default.vocTrainingFile2 = strTrainingFile2;
                Properties.Settings.Default.ExpandFiles = bExpandFiles;
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
            }
        }
    }
}
