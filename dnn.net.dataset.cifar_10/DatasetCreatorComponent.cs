using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.IO;
using System.Drawing;
using MyCaffe.basecode;
using MyCaffe.imagedb;
using MyCaffe.data;
using DNN.net.dataset.common;

namespace DNN.net.dataset.cifar_10
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
            get { return "CIFAR-10"; }
        }

        public void QueryConfiguration(DatasetConfiguration config)
        {
            string strTrainingBatch1 = "";
            string strTrainingBatch2 = "";
            string strTrainingBatch3 = "";
            string strTrainingBatch4 = "";
            string strTrainingBatch5 = "";
            string strTestingBatch = "";

            config.Settings.Add(new DataConfigSetting("Output Dataset Name", "CIFAR-10"));
            config.Settings.Add(new DataConfigSetting("Training Data File 1", strTrainingBatch1, DataConfigSetting.TYPE.FILENAME, "bin"));
            config.Settings.Add(new DataConfigSetting("Training Data File 2", strTrainingBatch2, DataConfigSetting.TYPE.FILENAME, "bin"));
            config.Settings.Add(new DataConfigSetting("Training Data File 3", strTrainingBatch3, DataConfigSetting.TYPE.FILENAME, "bin"));
            config.Settings.Add(new DataConfigSetting("Training Data File 4", strTrainingBatch4, DataConfigSetting.TYPE.FILENAME, "bin"));
            config.Settings.Add(new DataConfigSetting("Training Data File 5", strTrainingBatch5, DataConfigSetting.TYPE.FILENAME, "bin"));
            config.Settings.Add(new DataConfigSetting("Testing Data File", strTestingBatch, DataConfigSetting.TYPE.FILENAME, "bin"));
        }

        public void Create(DatasetConfiguration config, IXDatasetCreatorProgress progress)
        {
            string strDsName = config.Name;
            string strTrainingSrc = config.Name + ".training";
            string strTestingSrc = config.Name + ".testing";
            int nIdx = 0;
            int nTotal = 50000;

            m_bCancel = false;
            m_iprogress = progress;
            m_factory.DeleteSources(strTrainingSrc, strTestingSrc);

            Log log = new Log("CIFAR Dataset Creator");
            log.OnWriteLine += new EventHandler<LogArg>(log_OnWriteLine);

            try
            {
                DataConfigSetting dsTrainingDataFile1 = config.Settings.Find("Training Data File 1");
                DataConfigSetting dsTrainingDataFile2 = config.Settings.Find("Training Data File 2");
                DataConfigSetting dsTrainingDataFile3 = config.Settings.Find("Training Data File 3");
                DataConfigSetting dsTrainingDataFile4 = config.Settings.Find("Training Data File 4");
                DataConfigSetting dsTrainingDataFile5 = config.Settings.Find("Training Data File 5");
                DataConfigSetting dsTestingDataFile = config.Settings.Find("Testing Data File");

                if (dsTrainingDataFile1.Value.ToString().Length == 0)
                    throw new Exception("Training data file #1 name not specified!");

                if (dsTrainingDataFile2.Value.ToString().Length == 0)
                    throw new Exception("Training data file #2 name not specified!");

                if (dsTrainingDataFile3.Value.ToString().Length == 0)
                    throw new Exception("Training data file #3 name not specified!");

                if (dsTrainingDataFile4.Value.ToString().Length == 0)
                    throw new Exception("Training data file #4 name not specified!");

                if (dsTrainingDataFile5.Value.ToString().Length == 0)
                    throw new Exception("Training data file #5 name not specified!");

                if (dsTestingDataFile.Value.ToString().Length == 0)
                    throw new Exception("Testing data file name not specified!");

                log.WriteLine("Loading the data files...");

                if (m_bCancel)
                    return;

                int nTrainSrcId = m_factory.AddSource(strTrainingSrc, 3, 32, 32, false, 0);
                m_factory.Open(nTrainSrcId);

                log.WriteLine("Deleting existing data from '" + m_factory.OpenSource.Name + "'.");
                m_factory.DeleteSourceData();

                if (!loadFile(log, dsTrainingDataFile1, m_factory, nTotal, ref nIdx))
                    return;

                if (!loadFile(log, dsTrainingDataFile2, m_factory, nTotal, ref nIdx))
                    return;

                if (!loadFile(log, dsTrainingDataFile3, m_factory, nTotal, ref nIdx))
                    return;

                if (!loadFile(log, dsTrainingDataFile4, m_factory, nTotal, ref nIdx))
                    return;

                if (!loadFile(log, dsTrainingDataFile5, m_factory, nTotal, ref nIdx))
                    return;

                m_factory.UpdateSourceCounts();
                updateLabels(m_factory);
                m_factory.Close();

                int nTestSrcId = m_factory.AddSource(strTestingSrc, 3, 32, 32, false, 0);
                m_factory.Open(nTestSrcId);

                log.WriteLine("Deleting existing data from '" + m_factory.OpenSource.Name + "'.");
                m_factory.DeleteSourceData();

                nIdx = 0;
                nTotal = 10000;

                if (!loadFile(log, dsTestingDataFile, m_factory, nTotal, ref nIdx))
                    return;

                m_factory.UpdateSourceCounts();
                updateLabels(m_factory);
                m_factory.Close();

                log.WriteLine("Done loading training and testing data.");

                using (DNNEntities entities = EntitiesConnection.CreateEntities())
                {
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

                    Dataset ds = new Dataset();
                    ds.ImageHeight = rgSrcTraining[0].ImageHeight;
                    ds.ImageWidth = rgSrcTraining[0].ImageWidth;
                    ds.Name = strDsName;
                    ds.ImageEncoded = rgSrcTesting[0].ImageEncoded;
                    ds.ImageChannels = rgSrcTesting[0].ImageChannels;
                    ds.TestingPercent = (decimal)dfTestingPct;
                    ds.TestingSourceID = rgSrcTesting[0].ID;
                    ds.TestingTotal = rgSrcTesting[0].ImageCount;
                    ds.TrainingSourceID = rgSrcTraining[0].ID;
                    ds.TrainingTotal = rgSrcTraining[0].ImageCount;
                    ds.DatasetCreatorID = config.ID;
                    ds.DatasetGroupID = 0;
                    ds.ModelGroupID = 0;

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
                    log.WriteLine("ABORTED converting CIFAR data files.");
                else
                    log.WriteLine("Done converting CIFAR data files.");

                if (m_bCancel)
                {
                    m_iprogress.OnCompleted(new CreateProgressArgs(nIdx, nTotal, "ABORTED!", null, true));
                }
                else
                {
                    m_iprogress.OnCompleted(new CreateProgressArgs(1, "COMPLETED."));
                }
            }
        }

        void updateLabels(DatasetFactory factory)
        {
            Dictionary<int, string> rgLabels = new Dictionary<int, string>();

            rgLabels.Add(0, "airplane");
            rgLabels.Add(1, "automobile");
            rgLabels.Add(2, "bird");
            rgLabels.Add(3, "cat");
            rgLabels.Add(4, "deer");
            rgLabels.Add(5, "dog");
            rgLabels.Add(6, "frog");
            rgLabels.Add(7, "horse");
            rgLabels.Add(8, "ship");
            rgLabels.Add(9, "truck");

            for (int i = 0; i < rgLabels.Count; i++)
            {
                factory.UpdateLabelName(i, rgLabels[i]);
            }
        }

        bool loadFile(Log log, DataConfigSetting s, DatasetFactory factory, int nTotal, ref int nIdx)
        {
            Stopwatch sw = new Stopwatch();
            
            log.WriteLine("Loading '" + s.Name + "' into '" + factory.OpenSource.Name + "'...");

            sw.Start();

            FileStream fs = null;

            try
            {
                fs = new FileStream(s.Value.ToString(), FileMode.Open, FileAccess.Read);
                using (BinaryReader br = new BinaryReader(fs))
                {
                    fs = null;

                    for (int i = 0; i < 10000; i++)
                    {
                        int nLabel = (int)br.ReadByte();
                        byte[] rgImgBytes = br.ReadBytes(3072);
                        Bitmap img = createImage(rgImgBytes);

                        if (m_bCancel)
                            return false;

                        Datum d = ImageData.GetImageData(img, 3, false, nLabel);

                        factory.PutRawImageCache(nIdx, d);
                        nIdx++;

                        if (sw.ElapsedMilliseconds > 1000)
                        {
                            sw.Stop();
                            log.Progress = (double)nIdx / (double)nTotal;
                            log.WriteLine("Processing " + nIdx.ToString() + " of " + nTotal.ToString());
                            sw.Restart();
                        }
                    }

                    factory.ClearImageCash(true);
                }
            }
            finally
            {
                if (fs != null)
                    fs.Dispose();
            }

            return true;
        }

        private Bitmap createImage(byte[] rgImg)
        {
            int nRoffset = 0;
            int nGoffset = 1024;
            int nBoffset = 2048;
            int nX = 0;
            int nY = 0;

            Bitmap bmp = new Bitmap(32, 32);

            for (int i = 0; i < 1024; i++)
            {
                byte bR = rgImg[nRoffset + i];
                byte bG = rgImg[nGoffset + i];
                byte bB = rgImg[nBoffset + i];
                Color clr = Color.FromArgb(bR, bG, bB);

                bmp.SetPixel(nX, nY, clr);

                nX++;

                if (nX == 32)
                {
                    nY++;
                    nX = 0;
                }
            }

            return bmp;
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
