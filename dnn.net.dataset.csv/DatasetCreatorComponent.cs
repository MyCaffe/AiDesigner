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

namespace DNN.net.dataset.csv
{
    public partial class DatasetCreatorComponent : Component, IXDatasetCreator
    {
        IXDatasetCreatorProgress m_iprogress = null;
        DatasetFactory m_factory = new DatasetFactory();
        bool m_bCancel = false;
        List<SimpleDatum> m_rgImages = new List<SimpleDatum>();
        Schema m_schema;
        CsvParser m_parser = new CsvParser();
        IMGDB_VERSION m_imgDbVer = IMGDB_VERSION.DEFAULT;

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
            get { return "CSV"; }
        }

        public IMGDB_VERSION ImageDbVersion
        {
            get { return m_imgDbVer; }
            set { m_imgDbVer = value; }
        }

        public void QueryConfiguration(DatasetConfiguration config)
        {
            string strCsvFile = Properties.Settings.Default.CsvFile;
            string strTagCol = Properties.Settings.Default.TagCol;  // A
            string strDateCol = Properties.Settings.Default.DateCol; // B
            string strDataStartCol = Properties.Settings.Default.DataStartCol; // C
            string strDataEndCol = Properties.Settings.Default.DataEndCol; // AK
            string strLabelStartCol = Properties.Settings.Default.LabelStartCol; // AL
            string strLabelEndCol = Properties.Settings.Default.LabelEndCol; // AQ
            string strExtraStartCol = Properties.Settings.Default.ExtraStartCol; // AR
            string strExtraEndCol = Properties.Settings.Default.ExtraEndCol; // AS
            int nDataDescRow = Properties.Settings.Default.DataDescRow; // 6
            int nLabelStartRow = Properties.Settings.Default.LabelStartRow; // 7
            int nLabelEndRow = Properties.Settings.Default.LabelEndRow; // 8
            int nDataStartRow = Properties.Settings.Default.DataStartRow; // 10
            bool bUseBinary = Properties.Settings.Default.UseBinary; // true
            int nCellSize = Properties.Settings.Default.CellSize; // 5
            double dfTestingPct = Properties.Settings.Default.TestingPct; // 0.2 (20%)

            config.Settings.Add(new DataConfigSetting("Output Dataset Name", "CSV"));
            config.Settings.Add(new DataConfigSetting("CSV File", strCsvFile, DataConfigSetting.TYPE.FILENAME, "csv"));
            config.Settings.Add(new DataConfigSetting("Tag Col", strTagCol, DataConfigSetting.TYPE.TEXT));
            config.Settings.Add(new DataConfigSetting("Date Col", strDateCol, DataConfigSetting.TYPE.TEXT));
            config.Settings.Add(new DataConfigSetting("Data Start Col", strDataStartCol, DataConfigSetting.TYPE.TEXT));
            config.Settings.Add(new DataConfigSetting("Data End Col", strDataEndCol, DataConfigSetting.TYPE.TEXT));
            config.Settings.Add(new DataConfigSetting("Label Start Col", strLabelStartCol, DataConfigSetting.TYPE.TEXT));
            config.Settings.Add(new DataConfigSetting("Label End Col", strLabelEndCol, DataConfigSetting.TYPE.TEXT));
            config.Settings.Add(new DataConfigSetting("Extra Start Col", strExtraStartCol, DataConfigSetting.TYPE.TEXT));
            config.Settings.Add(new DataConfigSetting("Extra End Col", strExtraEndCol, DataConfigSetting.TYPE.TEXT));
            config.Settings.Add(new DataConfigSetting("Data Desc Row", nDataDescRow, DataConfigSetting.TYPE.INTEGER));
            config.Settings.Add(new DataConfigSetting("Label Start Row", nLabelStartRow, DataConfigSetting.TYPE.INTEGER));
            config.Settings.Add(new DataConfigSetting("Label End Row", nLabelEndRow, DataConfigSetting.TYPE.INTEGER));
            config.Settings.Add(new DataConfigSetting("Data Start Row", nDataStartRow, DataConfigSetting.TYPE.INTEGER));
            config.Settings.Add(new DataConfigSetting("Use Binary Values", (bUseBinary) ? 1 : 0, DataConfigSetting.TYPE.INTEGER));
            config.Settings.Add(new DataConfigSetting("Cell Size", nCellSize, DataConfigSetting.TYPE.INTEGER));
            config.Settings.Add(new DataConfigSetting("Testing Percentage", dfTestingPct, DataConfigSetting.TYPE.REAL));
        }

        public void Create(DatasetConfiguration config, IXDatasetCreatorProgress progress)
        {
            string strCsvFile = Properties.Settings.Default.CsvFile;
            string strDsName = config.Name;
            string strTrainingSrc = config.Name + ".training";
            string strTestingSrc = config.Name + ".testing";

            m_bCancel = false;
            m_iprogress = progress;
            m_factory.DeleteSources(strTrainingSrc, strTestingSrc);

            Log log = new Log("CSV Dataset Creator");
            log.OnWriteLine += new EventHandler<LogArg>(log_OnWriteLine);

            try
            {
                //-----------------------------------------
                //  Load the schema that defines the layout
                //  of the CSV file.
                //-----------------------------------------

                m_schema = loadSchema(config.Settings);


                //-----------------------------------------
                // Load and parse the CSV file.
                //-----------------------------------------

                DataConfigSetting dsCsvFile = config.Settings.Find("CSV File");

                strCsvFile = dsCsvFile.Value.ToString();
                if (strCsvFile.Length == 0)
                    throw new Exception("CSV data file name not specified!");

                log.WriteLine("Loading the data file...");

                if (m_bCancel)
                    return;

                m_parser.Load(strCsvFile, m_schema);


                //-----------------------------------------
                // Split the data into training and testing
                // sets.
                //-----------------------------------------

                List<DataItem> rgTraining = new List<DataItem>();
                List<DataItem> rgTesting = new List<DataItem>();

                DataConfigSetting dsPctTesting = config.Settings.Find("Testing Percentage");
                double dfVal = (double)dsPctTesting.Value;
                Random random = new Random();

                for (int i = 0; i < m_parser.Data.Count; i++)
                {
                    if (random.NextDouble() > dfVal)
                        rgTraining.Add(m_parser.Data[i]);
                    else
                        rgTesting.Add(m_parser.Data[i]);
                }

                Properties.Settings.Default.TestingPct = dfVal;


                //-----------------------------------------
                // Create the training data source.
                //-----------------------------------------

                int nCellHorizCount = 0;
                List<int> rgDim = getImageDim(m_parser, m_schema, out nCellHorizCount);
                int nTrainSrcId = m_factory.AddSource(strTrainingSrc, rgDim[0], rgDim[1], rgDim[2], false, 0);
                m_factory.Open(nTrainSrcId, 500, true); // use file based data.

                log.WriteLine("Deleting existing data from '" + m_factory.OpenSource.Name + "'.");
                m_factory.DeleteSourceData();

                if (!loadData(log, m_factory, m_parser, rgTraining, rgDim, true, true))
                    return;

                m_factory.UpdateSourceCounts();
                updateLabels(m_factory);

                log.WriteLine("Creating the image mean...");
                SimpleDatum dMean = SimpleDatum.CalculateMean(log, m_rgImages.ToArray(), new WaitHandle[] { new ManualResetEvent(false) });
                m_factory.PutRawImageMean(dMean, true);
                m_rgImages.Clear();

                m_factory.Close();


                //-----------------------------------------
                // Create the testing data source.
                //-----------------------------------------

                int nTestSrcId = m_factory.AddSource(strTestingSrc, rgDim[0], rgDim[1], rgDim[2], false, 0);
                m_factory.Open(nTestSrcId, 500, true); // use file based data.

                log.WriteLine("Deleting existing data from '" + m_factory.OpenSource.Name + "'.");
                m_factory.DeleteSourceData();

                if (!loadData(log, m_factory, m_parser, rgTesting, rgDim, false, false))
                    return;

                m_factory.UpdateSourceCounts();
                updateLabels(m_factory);
                m_factory.Close();


                //-----------------------------------------
                // Crate the data set.
                //-----------------------------------------

                log.WriteLine("Done loading training and testing data.");
                int nDatasetID = 0;

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

                    nDatasetID = ds.ID;
                }

                m_factory.SetDatasetParameter(nDatasetID, "PixelSize", m_schema.CellSize.ToString());
                m_factory.SetDatasetParameter(nDatasetID, "AttributeCount", m_parser.DataDescriptions.Count.ToString());
                m_factory.SetDatasetParameter(nDatasetID, "AttributeCountHoriz", nCellHorizCount.ToString());
                m_factory.SetDatasetParameter(nDatasetID, "AttributeCountVert", nCellHorizCount.ToString());
            }
            catch (Exception excpt)
            {
                log.WriteLine("ERROR: " + excpt.Message);
            }
            finally
            {
                Properties.Settings.Default.CsvFile = strCsvFile;
                Properties.Settings.Default.Save();

                if (m_bCancel)
                    log.WriteLine("ABORTED converting CSV data files.");
                else
                    log.WriteLine("Done converting CSV data files.");

                if (m_bCancel)
                {
                    m_iprogress.OnCompleted(new CreateProgressArgs(1, 1, "ABORTED!", null, true));
                }
                else
                {
                    m_iprogress.OnCompleted(new CreateProgressArgs(1, "COMPLETED."));
                }
            }
        }

        List<int> getImageDim(CsvParser p, Schema s, out int nSize)
        {
            int nDataCount = p.DataDescriptions.Count;
            int nCellSize = s.CellSize;

            double dfSize = Math.Sqrt(nDataCount);
            nSize = (int)Math.Ceiling(dfSize);

            int nFullSize = nSize * nCellSize;
            int nChannels = (s.BinaryValues) ? 1 : 3;

            return new List<int>() { nChannels, nFullSize, nFullSize };
        }

        void updateLabels(DatasetFactory factory)
        {
            for (int i=0; i<m_parser.Labels.Count; i++)
            {
                factory.UpdateLabelName(i, m_parser.Labels[i]);
            }
        }

        Schema loadSchema(DataConfigSettingCollection col)
        {
            string strTagCol = getConfigVal(col, "Tag Col");
            string strDateCol = getConfigVal(col, "Date Col");
            string strDataStartCol = getConfigVal(col, "Data Start Col");
            string strDataEndCol = getConfigVal(col, "Data End Col");
            string strLabelStartCol = getConfigVal(col, "Label Start Col");
            string strLabelEndCol = getConfigVal(col, "Label End Col");
            string strExtraStartCol = getConfigVal(col, "Extra Start Col");
            string strExtraEndCol = getConfigVal(col, "Extra End Col");
            int nTagCol = SchemaColumn.ConvertCol(strTagCol);
            int nDateCol = SchemaColumn.ConvertCol(strDateCol);
            int nDataStartCol = SchemaColumn.ConvertCol(strDataStartCol);
            int nDataEndCol = SchemaColumn.ConvertCol(strDataEndCol);
            int nLabelStartCol = SchemaColumn.ConvertCol(strLabelStartCol);
            int nLabelEndCol = SchemaColumn.ConvertCol(strLabelEndCol);
            int nExtraStartCol = SchemaColumn.ConvertCol(strExtraStartCol);
            int nExtraEndCol = SchemaColumn.ConvertCol(strExtraEndCol);
            SchemaColumn colS = new SchemaColumn(nTagCol, nDateCol, nDataStartCol, nDataEndCol, nLabelStartCol, nLabelEndCol, nExtraStartCol, nExtraEndCol);

            int nDataDescRow = getConfigValAsInt(col, "Data Desc Row");
            int nLabelStartRow = getConfigValAsInt(col, "Label Start Row");
            int nLabelEndRow = getConfigValAsInt(col, "Label End Row");
            int nDataStartRow = getConfigValAsInt(col, "Data Start Row");
            SchemaRow rowS = new SchemaRow(nDataDescRow, nLabelStartRow, nLabelEndRow, nDataStartRow);

            DataConfigSetting s = col.Find("Use Binary Values");
            bool bBinaryValues = ((int)s.Value == 0) ? false : true;

            s = col.Find("Cell Size");
            int nCellSize = (int)s.Value;

            Properties.Settings.Default.TagCol = strTagCol;
            Properties.Settings.Default.DateCol = strDateCol;
            Properties.Settings.Default.DataStartCol = strDataStartCol;
            Properties.Settings.Default.DataEndCol = strDataEndCol;
            Properties.Settings.Default.LabelStartCol = strLabelStartCol;
            Properties.Settings.Default.LabelEndCol = strLabelEndCol;
            Properties.Settings.Default.ExtraStartCol = strExtraStartCol;
            Properties.Settings.Default.ExtraEndCol = strExtraEndCol;
            Properties.Settings.Default.DataDescRow = nDataDescRow + 1;
            Properties.Settings.Default.LabelStartRow = nLabelStartRow + 1;
            Properties.Settings.Default.LabelEndRow = nLabelEndRow + 1;
            Properties.Settings.Default.DataStartRow = nDataStartRow + 1;
            Properties.Settings.Default.UseBinary = bBinaryValues;
            Properties.Settings.Default.CellSize = nCellSize;
            Properties.Settings.Default.Save();

            return new Schema(colS, rowS, bBinaryValues, nCellSize);
        }

        string getConfigVal(DataConfigSettingCollection col, string strName)
        {
            DataConfigSetting s = col.Find(strName);
            if (s == null)
                throw new Exception("Could not find the setting '" + strName + "'!");

            return s.Value.ToString();
        }

        int getConfigValAsInt(DataConfigSettingCollection col, string strName)
        {
            DataConfigSetting s = col.Find(strName);
            if (s == null)
                throw new Exception("Could not find the setting '" + strName + "'!");

            return (int)s.Value - 1;
        }

        bool loadData(Log log, DatasetFactory factory, CsvParser parser, List<DataItem> rgData, List<int> rgDim, bool bTraining, bool bCreateImageMean)
        {
            Stopwatch sw = new Stopwatch();

            log.WriteLine("Loading data into '" + factory.OpenSource.Name + "'...");
            sw.Start();

            m_rgImages.Clear();

            int nSize = rgDim[1] / m_schema.CellSize;

            for (int i = 0; i < rgData.Count; i++)
            {
                int nLabel = rgData[i].Label;
                Bitmap bmp = createImage(rgData[i].Data, nSize, m_schema.CellSize, m_schema.BinaryValues);

                if (m_bCancel)
                    return false;

                Datum d = ImageData.GetImageDataD(bmp, rgDim[0], false, nLabel);
                factory.PutRawImageCache(i, d);

                if (bCreateImageMean)
                    m_rgImages.Add(new SimpleDatum(d));

                if (sw.ElapsedMilliseconds > 1000)
                {
                    sw.Stop();
                    log.Progress = (double)i / (double)rgData.Count;
                    log.WriteLine("Processing " + i.ToString() + " of " + rgData.Count.ToString());
                    sw.Restart();
                    factory.ClearImageCashe(true);
                }
            }

            factory.ClearImageCashe(true);

            return true;
        }

        Color getColor(double dfVal, ColorMapper mapper, bool bBinary)
        {
            Color clr = Color.Black;

            if (bBinary)
            {
                if (dfVal != 0)
                    clr = Color.White;
            }
            else
            {
                clr = mapper.GetColor(dfVal);
            }

            return clr;
        }

        Bitmap createImage(List<double> rgVal, int nSize, int nCellSize, bool bBinary)
        {
            Dictionary<Color, Brush> rgBrushes = new Dictionary<Color, Brush>();
            ColorMapper mapper = null;
            Bitmap bmp = new Bitmap(nSize * nCellSize, nSize * nCellSize);
            int nIdx = 0;

            if (!bBinary)
            {
                double dfMin = rgVal.Min(p => p);
                double dfMax = rgVal.Max(p => p);
                mapper = new ColorMapper(dfMin, dfMax, Color.Black, Color.Fuchsia);
            }

            using (Graphics g = Graphics.FromImage(bmp))
            {
                g.FillRectangle(Brushes.Black, new Rectangle(0, 0, bmp.Width, bmp.Height));

                if (nCellSize > 1)
                {
                    for (int y = 0; y < nSize; y++)
                    {
                        for (int x = 0; x < nSize; x++)
                        {
                            if (nIdx < rgVal.Count)
                            {
                                Color clr = getColor(rgVal[nIdx], mapper, bBinary);
                                Rectangle rcCell = new Rectangle(x * nCellSize, y * nCellSize, nCellSize, nCellSize);

                                if (!rgBrushes.ContainsKey(clr))
                                    rgBrushes.Add(clr, new SolidBrush(clr));

                                g.FillRectangle(rgBrushes[clr], rcCell);

                                nIdx++;
                            }
                        }
                    }
                }
            }

            if (nCellSize == 1)
            {
                for (int y = 0; y < nSize; y++)
                {
                    for (int x = 0; x < nSize; x++)
                    {
                        if (nIdx < rgVal.Count)
                        {
                            Color clr = getColor(rgVal[nIdx], mapper, bBinary);
                            bmp.SetPixel(x, y, clr);
                            nIdx++;
                        }
                    }
                }
            }

            foreach (KeyValuePair<Color, Brush> val in rgBrushes)
            {
                val.Value.Dispose();
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
