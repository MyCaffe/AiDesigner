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

    /// <summary>
    /// Defines the period type.
    /// </summary>
    public enum PERIOD_TYPE
    {
        /// <summary>
        /// Specifies the daily period.
        /// </summary>
        DAY,
        /// <summary>
        /// Specifies the 1 minute period.
        /// </summary>
        MIN_1,
        /// <summary>
        /// Specifies the 5 minute period.
        /// </summary>
        MIN_5,
        /// <summary>
        /// Specifies the 15 minute period.
        /// </summary>
        MIN_15,
        /// <summary>
        /// Specifies the 60 minute period.
        /// </summary>
        MIN_60,
        /// <summary>
        /// Specifies the 10 second period.
        /// </summary>
        SEC_10,
        /// <summary>
        /// Specifies the 20 second period.
        /// </summary>
        SEC_20,
        /// <summary>
        /// Specifies the 30 second period.
        /// </summary>
        SEC_30
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
        /// <summary>
        /// Get the total number of assets available.
        /// </summary>
        /// <param name="strRebalanceName">Specifies a rebalance name, if exists.</param>
        /// <param name="rgParam">Specifies special parameters (optional).</param>
        /// <returns>The total number of assets is returned.</returns>
        int GetTotalAssets(string strRebalanceName, Dictionary<string, string> rgParam = null);
        /// <summary>
        /// Get the asset ID at a given index.
        /// </summary>
        /// <param name="nIdx">Specifies the index.</param>
        /// <returns>The asset ID is returned.</returns>
        int GetAssetIDAt(int nIdx);
        /// <summary>
        /// Get the plots for a given asset, period and signal.
        /// </summary>
        /// <param name="nAssetID">Specifies the asset ID.</param>
        /// <param name="period">Specifies the period.</param>
        /// <param name="strSignal">Specifies the signal to fill the price data with.</param>
        /// <returns>A plot collection of data is returned.</returns>
        PlotCollection GetPlots(int nAssetID, PERIOD_TYPE period, string strSignal);
        /// <summary>
        /// Get all signal plots for a given asset and period.
        /// </summary>
        /// <param name="nAssetID">Specifies the assetID</param>
        /// <param name="period">Specifies the period.</param>
        /// <returns>A PlotCollectionSet with all plots is returned.</returns>
        PlotCollectionSet GetAllPlots(int nAssetID, PERIOD_TYPE period);
        /// <summary>
        /// Perform any clean up required.
        /// </summary>
        void CleanUp();
    }

    /// <summary>
    /// The IXDatasetDataQueryEx interface extends the base interface and allows for additional data queries.
    /// </summary>
    public interface IXDatasetDataQueryEx : IXDatasetDataQuery
    {
        /// <summary>
        /// Returns true if the GetImages method is to be used instead of the GetPlots method.
        /// </summary>
        bool UseImages { get; }
        /// <summary>
        /// Return the valid date time range for a given asset.
        /// </summary>
        /// <param name="nAssetID">Specifies the asset ID.</param>
        /// <returns>A tuple of the start and end date of valid time for the asset is returned.</returns>
        Tuple<DateTime, DateTime> GetValidDateTimeRange(int nAssetID);
        /// <summary>
        /// Set the current date time.
        /// </summary>
        /// <param name="dt">Specifies the time to set.</param>
        void SetDateTime(DateTime dt);
        /// <summary>
        /// Get a list of images for a given asset, period and signal.
        /// </summary>
        /// <param name="nAssetID">Specifies the asset ID.</param>
        /// <param name="period">Specifies the period.</param>
        /// <param name="strSignal">Specifies the signal.</param>
        /// <param name="nPastCount">Specifies the number of past data points to include in the image and plots returned.  NOTE: The index returned sits at the very end of the past plots.</param>
        /// <param name="nFutureCount">Specifies the number of future data points to include in the plots returned.</param>
        /// <returns>A tuple of the date time, image, plot collection and current index is returned.</returns>
        List<Tuple<DateTime, Image, PlotCollection, int>> GetImages(int nAssetID, PERIOD_TYPE period, string strSignal, int nPastCount, int nFutureCount);
    }
}
