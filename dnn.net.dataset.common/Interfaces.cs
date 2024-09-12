using MyCaffe.basecode;
using SimpleGraphing;
using System;
using System.Collections.Generic;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DNN.net.dataset.common
{
    /// <summary>
    /// Defines the type of custom settings to use.
    /// </summary>
    public enum CUSTOM_SETTING
    {
        /// <summary>
        /// An unknown custom setting specified.
        /// </summary>
        UNKNOWN,
        /// <summary>
        /// Load a set of GPH files.
        /// </summary>
        LOAD_GPH,
        /// <summary>
        /// Load the image configuration.
        /// </summary>
        LOAD_IMAGE_CONFIG,
        /// <summary>
        /// Show a help window.
        /// </summary>
        HELP,
        /// <summary>
        /// Show the help file if exists.
        /// </summary>
        HELP_FILE
    }

    public enum PERIOD_TYPE
    {
        /// <summary>
        /// The period is a day.
        /// </summary>
        DAY,
        /// <summary>
        /// The period is 1 minute.
        /// </summary>
        MIN_1,
        /// <summary>
        /// The period is 5 minutes.
        /// </summary>
        MIN_5,
        /// <summary>
        /// The period is 15 minutes.
        /// </summary>
        MIN_15,
        /// <summary>
        /// This period is 60 minutes.
        /// </summary>
        MIN_60
    }

    /// <summary>
    /// The IXDatasetCreatorCuda interface should be implemented by dataset creators that use CUDA.
    /// </summary>
    public interface IXDatasetCreatorCuda
    {
        void Initialize(string strCuda);
    }

    /// <summary>
    /// The IXDatasetCreator interface is used by the SignalPop AI Designer to create datasets.
    /// </summary>
    public interface IXDatasetCreator
    {
        string Name { get; }
        DB_VERSION DbVersion { get; set; }
        void QueryConfiguration(DatasetConfiguration config);
        void Create(DatasetConfiguration config, IXDatasetCreatorProgress progress);
    }

    /// <summary>
    /// The IXDatasetCreatorEx interface extends the base interface and allows support for custom datasets.
    /// </summary>
    public interface IXDatasetCreatorEx : IXDatasetCreator
    {
        Dictionary<string, string> QueryItems(string strItemType, Dictionary<string, string> rgCustomParam, CancelEvent evtCancel);
        bool UpdateItems(string strItemType, List<int> rgID, List<string> rgParam, List<byte[]> rgData, CancelEvent evtCancel);
        bool RunWorkPackage(int nWpID, string strType, string strName, string strConfig, Dictionary<string, string> rgWpParam, CancelEvent evtCancel, IXDatasetCreatorProgress progress);
    }

    /// <summary>
    /// Optional interface to view the Debug data or Data Criteria data as an image.
    /// </summary>
    public interface IXDatasetViewer
    {
        SimpleDatum.DATA_FORMAT[] SupportedDebugDataFormats { get; }
        Image ViewDebugData(byte[] rgData, SimpleDatum.DATA_FORMAT fmt, out byte[] rgExtra, int nSrcID = 0, int nDatasetID = 0, Dictionary<string, double> rgParam = null);
        SimpleDatum.DATA_FORMAT[] SupportedDataCriteriaFormats { get; }
        Image ViewDataCriteria(byte[] rgData, SimpleDatum.DATA_FORMAT fmt, out byte[] rgExtra, int nSrcID = 0, int nDatasetID = 0, Dictionary<string, double> rgParam = null);
    }

    /// <summary>
    /// Optional interface used to retrieve the recommended label for a given data item.
    /// </summary>
    public interface IXDatasetViewer2 : IXDatasetViewer
    {
        int GetRecommendedLabel(byte[] rgData, SimpleDatum.DATA_FORMAT fmt, int nSrcID = 0);
        string GetRecommendedLabelName(int nLabel);
    }

    /// <summary>
    /// Optional interface used to retrieve an output image based on user supplied input data.
    /// </summary>
    public interface IXDataImage
    {
        byte[] LoadData(int nDatasetID, DateTime dtStart, DateTime dtEnd, Log log = null, Dictionary<string, string> rgParam = null);
        byte[] LoadData(int nDatasetID, DateTime dt, int nCount = -1, Log log = null, Dictionary<string, string> rgParam = null);
        byte[] LoadNext(int nSteps, bool bVerbose);
        byte[] LoadNext(DateTime dt, bool bVerbose);
        Bitmap GetCurrentImage(out DateTime dt, out bool bBadImage);
        string GetConfigurationSettings();
    }

    public interface IXDatasetCreatorSettings
    {
        void VerifyConfiguration(DataConfigSetting[] settings);
        void GetCustomSetting(string strName, string strCustomSettingType, DataConfigSetting[] settings);
    }

    public interface IXDatasetCreatorProgress
    {
        void OnProgress(CreateProgressArgs args);
        void OnCompleted(CreateProgressArgs args);
    }

    /// <summary>
    /// The IXDatasetDataQuery interface is implemented by raw data provider plug-ins used by dataset creators.
    /// </summary>
    public interface IXDatasetDataQuery
    {
        int GetTotalAssets(string strRebalanceName);
        int GetAssetIDAt(int nIdx);
        PlotCollection GetPlots(int nAssetID, PERIOD_TYPE period, string strSignal);
        void CleanUp();
    }
}
