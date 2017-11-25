using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Text;
using MyCaffe.imagedb;
using MyCaffe.basecode;
using DNN.net.dataset.common;

namespace DNN.net.dataset.mnist
{
    public partial class DatasetCreatorComponent : Component, IXDatasetCreator
    {
        IXDatasetCreatorProgress m_iprogress = null;
        DatasetFactory m_factory = new DatasetFactory();
        bool m_bCancel = false;

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
            get { return "MNIST"; }
        }

        public void QueryConfiguration(DatasetConfiguration config)
        {
            string strTrainingDataFile = "";
            string strTrainingLabelFile = "";
            string strTestingDataFile = "";
            string strTestingLabelFile = "";

            config.Settings.Add(new DataConfigSetting("Output Dataset Name", "MNIST"));
            config.Settings.Add(new DataConfigSetting("Testing Data File", strTestingDataFile, DataConfigSetting.TYPE.FILENAME, "gz"));
            config.Settings.Add(new DataConfigSetting("Testing Label File", strTestingLabelFile, DataConfigSetting.TYPE.FILENAME, "gz"));
            config.Settings.Add(new DataConfigSetting("Training Data File", strTrainingDataFile, DataConfigSetting.TYPE.FILENAME, "gz"));
            config.Settings.Add(new DataConfigSetting("Training Label File", strTrainingLabelFile, DataConfigSetting.TYPE.FILENAME, "gz"));
            config.Settings.Add(new DataConfigSetting("Channels", "1", DataConfigSetting.TYPE.INTEGER));
        }

        public void Create(DatasetConfiguration config, IXDatasetCreatorProgress progress)
        {
            string strDsName = config.Name;
            string strTrainingSrc = config.Name + ".training";
            string strTestingSrc = config.Name + ".testing";

            DataConfigSetting dsChannels = config.Settings.Find("Channels");
            int nChannels = 1;

            if (int.TryParse(dsChannels.Value.ToString(), out nChannels))
            {
                if (nChannels != 1 && nChannels != 3)
                    nChannels = 1;
            }

            m_iprogress = progress;

            if (nChannels == 3)
            {
                strTrainingSrc += "." + "3_ch";
                strTestingSrc += "." + "3_ch";
                strDsName += "." + "3_ch";
            }

            m_factory.DeleteSources(strTrainingSrc, strTestingSrc);

            Log log = new Log("MNist Dataset Creator");
            log.OnWriteLine += new EventHandler<LogArg>(log_OnWriteLine);

            try
            {
                MgrMnistData mgr = new MgrMnistData(m_factory, log);
                mgr.OnLoadError += new EventHandler<LoadErrorArgs>(mgr_OnLoadError);
                mgr.OnLoadProgress += new EventHandler<LoadArgs>(mgr_OnLoadProgress);

                DataConfigSetting dsTestingDataFile = config.Settings.Find("Testing Data File");
                DataConfigSetting dsTestingLabelFile = config.Settings.Find("Testing Label File");
                DataConfigSetting dsTrainingDataFile = config.Settings.Find("Training Data File");
                DataConfigSetting dsTrainingLabelFile = config.Settings.Find("Training Label File");

                if (dsTrainingDataFile.Value.ToString().Length == 0)
                    throw new Exception("Training data file name not specified!");

                if (dsTrainingLabelFile.Value.ToString().Length == 0)
                    throw new Exception("Training label file name not specified!");

                if (dsTestingDataFile.Value.ToString().Length == 0)
                    throw new Exception("Testing data file name not specified!");

                if (dsTestingLabelFile.Value.ToString().Length == 0)
                    throw new Exception("Testing label file name not specified!");

                log.WriteLine("Converting the training data files...");

                if (m_bCancel)
                    return;

                mgr.ConvertData(dsTrainingDataFile.Value.ToString(), dsTrainingLabelFile.Value.ToString(), strTrainingSrc, true, false, nChannels);

                if (m_bCancel)
                    return;

                log.WriteLine("Converting the testing data files...");

                if (m_bCancel)
                    return;


                mgr.ConvertData(dsTestingDataFile.Value.ToString(), dsTestingLabelFile.Value.ToString(), strTestingSrc, false, false, nChannels);

                if (m_bCancel)
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

                    DataConfigSetting dsName = config.Settings.Find("Output Dataset Name");
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
                if (m_bCancel)
                    log.WriteLine("ABORTED converting MNIST data files.");
                else
                    log.WriteLine("Done converting MNIST data files.");

                if (m_bCancel)
                {
                    m_iprogress.OnCompleted(new CreateProgressArgs(1, "ABORTED!", null, true));
                }
                else
                {
                    m_iprogress.OnCompleted(new CreateProgressArgs(1, "COMPLETED."));
                }
            }
        }

        void mgr_OnLoadProgress(object sender, LoadArgs e)
        {
            if (m_bCancel)
            {
                e.Cancel = true;
                return;
            }
        }

        void mgr_OnLoadError(object sender, LoadErrorArgs e)
        {
            if (m_iprogress != null)
            {
                CreateProgressArgs arg = new CreateProgressArgs(0, "ERROR", new Exception(e.Error));
                m_iprogress.OnProgress(arg);
                m_bCancel = true;
            }
        }

        void log_OnWriteLine(object sender, LogArg e)
        {
            if (m_iprogress != null)
            {
                CreateProgressArgs arg = new CreateProgressArgs(e.Progress, e.Message);
                m_iprogress.OnProgress(arg);

                if (arg.Abort)
                    m_bCancel = true;
            }
        }
    }
}
