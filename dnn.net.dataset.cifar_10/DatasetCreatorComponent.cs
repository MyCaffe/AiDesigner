using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.IO;
using System.Drawing;
using MyCaffe.basecode;
using MyCaffe.db.image;
using MyCaffe.data;
using DNN.net.dataset.common;
using System.Threading;

namespace DNN.net.dataset.cifar_10
{
    public partial class DatasetCreatorComponent : Component, IXDatasetCreator
    {
        IXDatasetCreatorProgress m_iprogress = null;
        DatasetFactory m_factory = new DatasetFactory();
        bool m_bCancel = false;
        List<SimpleDatum> m_rgImages = new List<SimpleDatum>();

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
            string strTrainingBatchFile1 = Properties.Settings.Default.TrainingDataFile1;
            string strTrainingBatchFile2 = Properties.Settings.Default.TrainingDataFile2;
            string strTrainingBatchFile3 = Properties.Settings.Default.TrainingDataFile3;
            string strTrainingBatchFile4 = Properties.Settings.Default.TrainingDataFile4;
            string strTrainingBatchFile5 = Properties.Settings.Default.TrainingDataFile5;
            string strTestingBatchFile = Properties.Settings.Default.TestingDataFile;

            config.Settings.Add(new DataConfigSetting("Output Dataset Name", "CIFAR-10"));
            config.Settings.Add(new DataConfigSetting("Training Data File 1", strTrainingBatchFile1, DataConfigSetting.TYPE.FILENAME, "bin"));
            config.Settings.Add(new DataConfigSetting("Training Data File 2", strTrainingBatchFile2, DataConfigSetting.TYPE.FILENAME, "bin"));
            config.Settings.Add(new DataConfigSetting("Training Data File 3", strTrainingBatchFile3, DataConfigSetting.TYPE.FILENAME, "bin"));
            config.Settings.Add(new DataConfigSetting("Training Data File 4", strTrainingBatchFile4, DataConfigSetting.TYPE.FILENAME, "bin"));
            config.Settings.Add(new DataConfigSetting("Training Data File 5", strTrainingBatchFile5, DataConfigSetting.TYPE.FILENAME, "bin"));
            config.Settings.Add(new DataConfigSetting("Testing Data File", strTestingBatchFile, DataConfigSetting.TYPE.FILENAME, "bin"));
        }

        public void Create(DatasetConfiguration config, IXDatasetCreatorProgress progress)
        {
            string strTrainingBatchFile1 = Properties.Settings.Default.TrainingDataFile1;
            string strTrainingBatchFile2 = Properties.Settings.Default.TrainingDataFile2;
            string strTrainingBatchFile3 = Properties.Settings.Default.TrainingDataFile3;
            string strTrainingBatchFile4 = Properties.Settings.Default.TrainingDataFile4;
            string strTrainingBatchFile5 = Properties.Settings.Default.TrainingDataFile5;
            string strTestingBatchFile = Properties.Settings.Default.TestingDataFile;
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

                strTrainingBatchFile1 = dsTrainingDataFile1.Value.ToString();
                if (strTrainingBatchFile1.Length == 0)
                    throw new Exception("Training data file #1 name not specified!");

                strTrainingBatchFile2 = dsTrainingDataFile2.Value.ToString();
                if (strTrainingBatchFile2.Length == 0)
                    throw new Exception("Training data file #2 name not specified!");

                strTrainingBatchFile3 = dsTrainingDataFile3.Value.ToString();
                if (strTrainingBatchFile3.Length == 0)
                    throw new Exception("Training data file #3 name not specified!");

                strTrainingBatchFile4 = dsTrainingDataFile4.Value.ToString();
                if (strTrainingBatchFile4.Length == 0)
                    throw new Exception("Training data file #4 name not specified!");

                strTrainingBatchFile5 = dsTrainingDataFile5.Value.ToString();
                if (strTrainingBatchFile5.Length == 0)
                    throw new Exception("Training data file #5 name not specified!");

                strTestingBatchFile = dsTestingDataFile.Value.ToString();
                if (strTestingBatchFile.Length == 0)
                    throw new Exception("Testing data file name not specified!");

                log.WriteLine("Loading the data files...");

                if (m_bCancel)
                    return;

                int nTrainSrcId = m_factory.AddSource(strTrainingSrc, 3, 32, 32, false, 0);
                m_factory.Open(nTrainSrcId, 500, true); // use file based data.

                log.WriteLine("Deleting existing data from '" + m_factory.OpenSource.Name + "'.");
                m_factory.DeleteSourceData();

                if (!loadFile(log, dsTrainingDataFile1.Name, strTrainingBatchFile1, m_factory, nTotal, true, ref nIdx))
                    return;

                if (!loadFile(log, dsTrainingDataFile2.Name, strTrainingBatchFile2, m_factory, nTotal, true, ref nIdx))
                    return;

                if (!loadFile(log, dsTrainingDataFile3.Name, strTrainingBatchFile3, m_factory, nTotal, true, ref nIdx))
                    return;

                if (!loadFile(log, dsTrainingDataFile4.Name, strTrainingBatchFile4, m_factory, nTotal, true, ref nIdx))
                    return;

                if (!loadFile(log, dsTrainingDataFile5.Name, strTrainingBatchFile5, m_factory, nTotal, true, ref nIdx))
                    return;

                m_factory.UpdateSourceCounts();
                updateLabels(m_factory);

                log.WriteLine("Creating the image mean...");
                SimpleDatum dMean = SimpleDatum.CalculateMean(log, m_rgImages.ToArray(), new WaitHandle[] { new ManualResetEvent(false) });
                m_factory.PutRawImageMean(dMean, true);
                m_rgImages.Clear();

                m_factory.Close();

                int nTestSrcId = m_factory.AddSource(strTestingSrc, 3, 32, 32, false, 0);
                m_factory.Open(nTestSrcId, 500, true); // use file based data.

                log.WriteLine("Deleting existing data from '" + m_factory.OpenSource.Name + "'.");
                m_factory.DeleteSourceData();

                nIdx = 0;
                nTotal = 10000;

                if (!loadFile(log, dsTestingDataFile.Name, strTestingBatchFile, m_factory, nTotal, false, ref nIdx))
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
                Properties.Settings.Default.TrainingDataFile1 = strTrainingBatchFile1;
                Properties.Settings.Default.TrainingDataFile2 = strTrainingBatchFile2;
                Properties.Settings.Default.TrainingDataFile3 = strTrainingBatchFile3;
                Properties.Settings.Default.TrainingDataFile4 = strTrainingBatchFile4;
                Properties.Settings.Default.TrainingDataFile5 = strTrainingBatchFile5;
                Properties.Settings.Default.TestingDataFile = strTestingBatchFile;
                Properties.Settings.Default.Save();

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

        bool loadFile(Log log, string strName, string strFile, DatasetFactory factory, int nTotal, bool bCreateMeanImage, ref int nIdx)
        {
            Stopwatch sw = new Stopwatch();
            
            log.WriteLine("Loading '" + strName + "' into '" + factory.OpenSource.Name + "'...");

            sw.Start();

            FileStream fs = null;

            try
            {
                fs = new FileStream(strFile, FileMode.Open, FileAccess.Read);
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

                        if (bCreateMeanImage)
                            m_rgImages.Add(new SimpleDatum(d));

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
