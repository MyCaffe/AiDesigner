using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;
using System.IO;
using System.Windows.Forms;
using System.Drawing.Design;
using System.Windows.Forms.Design;
using System.Drawing;
using MyCaffe.basecode;

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
    /// The IXDatasetCreator interface is used by the SignalPop AI Designer to create datasets.
    /// </summary>
    public interface IXDatasetCreator
    {
        string Name { get; }
        IMGDB_VERSION ImageDbVersion { get; set; }
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
        Image ViewDebugData(byte[] rgData, SimpleDatum.DATA_FORMAT fmt, out byte[] rgExtra, int nSrcID = 0, int nDatasetID = 0, int nMaxItems = 0);
        SimpleDatum.DATA_FORMAT[] SupportedDataCriteriaFormats { get; }
        Image ViewDataCriteria(byte[] rgData, SimpleDatum.DATA_FORMAT fmt, out byte[] rgExtra, int nSrcID = 0, int nDatasetID = 0, int nMaxItems = 0);
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
        byte[] LoadNext(int nSteps);
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

    [Serializable]
    public class DatasetConfiguration 
    {
        string m_strName = "";
        int m_nID = 0;
        DataConfigSettingCollection m_rgSettings = new DataConfigSettingCollection();
        string m_strSelectedGroup = "";
        bool m_bReadOnly = false;

        public DatasetConfiguration(string strName, int nID, string strSelectedGroup)
        {
            m_strName = strName;
            m_nID = nID;
            m_strSelectedGroup = strSelectedGroup;
        }

        [Browsable(false)]
        public bool IsReadOnly
        {
            get { return m_bReadOnly; }
            set { m_bReadOnly = value; }
        }

        [ReadOnly(true)]
        public int ID
        {
            get { return m_nID; }
            set { m_nID = value; }
        }

        [ReadOnly(true)]
        public string Name
        {
            get { return m_strName; }
        }

        public string SelectedGroup
        {
            get { return m_strSelectedGroup; }
        }

        [TypeConverter(typeof(ExpandableObjectConverter)), ReadOnly(false)]
        public DataConfigSettingCollection Settings
        {
            get { return m_rgSettings; }
        }

        public void Sort()
        {
            m_rgSettings.Sort();
        }

        public DatasetConfiguration Clone()
        {
            DatasetConfiguration config = new DatasetConfiguration(m_strName, m_nID, m_strSelectedGroup);

            config.m_rgSettings = m_rgSettings.Clone();

            return config;
        }

        public static void LoadFromDirectory(DataConfigSetting[] settings, string strExt, string strPath = "c:\\temp\\configurations")
        {
            FolderBrowserDialog dlg = new FolderBrowserDialog();

            dlg.RootFolder = Environment.SpecialFolder.MyComputer;
            dlg.SelectedPath = strPath;

            if (dlg.ShowDialog() == DialogResult.OK)
            {
                int nFirstFile = -1;

                for (int i = 0; i < settings.Length; i++)
                {
                    if (settings[i].Name.Contains(strExt.ToUpper()))
                    {
                        if (nFirstFile == -1)
                            nFirstFile = i;

                        settings[i].Value = "";
                    }
                }

                string[] rgstrFiles = Directory.GetFiles(dlg.SelectedPath);
                int nIdx = 0;

                while (nIdx < rgstrFiles.Length && !rgstrFiles[nIdx].Contains("." + strExt.ToLower()))
                {
                    nIdx++;
                }

                for (int i = nFirstFile; i < settings.Length; i++)
                {
                    if (nIdx < rgstrFiles.Length && rgstrFiles[nIdx].Contains("." + strExt.ToLower()))
                    {
                        settings[i].Value = rgstrFiles[nIdx];
                        nIdx++;
                    }

                    while (nIdx < rgstrFiles.Length && !rgstrFiles[nIdx].Contains("." + strExt.ToLower()))
                    {
                        nIdx++;
                    }

                    if (nIdx >= rgstrFiles.Length || !rgstrFiles[nIdx].Contains("." + strExt.ToLower()))
                        break;
                }
            }
        }

        public static void SaveToFile(DataConfigSetting[] settings, string strFile)
        {
            using (StreamWriter sw = new StreamWriter(strFile))
            {
                sw.WriteLine("Count;" + settings.Length.ToString());

                for (int i = 0; i < settings.Length; i++)
                {
                    sw.WriteLine(settings[i].ToSaveString());
                }
            }
        }

        public static DataConfigSetting[] LoadFromFile(string strFile)
        {
            List<DataConfigSetting> rgSettings = new List<DataConfigSetting>();

            using (StreamReader sr = new StreamReader(strFile))
            {
                string strCount = sr.ReadLine();
                int nPos = strCount.IndexOf(';');
                if (nPos < 0)
                    throw new Exception("Missing 'Count;#'!");

                strCount = strCount.Substring(nPos + 1);
                int nCount = 0;

                if (!int.TryParse(strCount, out nCount))
                    throw new Exception("Missing 'Count;#'!");

                for (int i = 0; i < nCount; i++)
                {
                    rgSettings.Add(DataConfigSetting.Parse(sr.ReadLine()));
                }
            }

            return rgSettings.ToArray();
        }
    }

    [Serializable]
    public class DataConfigSettingCollection : IEnumerable<DataConfigSetting>
    {
        List<DataConfigSetting> m_rgSettings = new List<DataConfigSetting>();

        public DataConfigSettingCollection()
        {
        }

        public void Sort()
        {
            m_rgSettings.Sort(new Comparison<DataConfigSetting>(sort));
        }

        private int sort(DataConfigSetting a, DataConfigSetting b)
        {
            return a.Name.CompareTo(b.Name);
        }

        public int Count
        {
            get { return m_rgSettings.Count; }
        }

        public DataConfigSetting[] Items
        {
            get { return m_rgSettings.ToArray(); }
            set
            {
                m_rgSettings.Clear();

                foreach (DataConfigSetting s in value)
                {
                    m_rgSettings.Add(s.Clone());
                }
            }
        }

        public DataConfigSetting this[int nIdx]
        {
            get { return m_rgSettings[nIdx]; }
            set { m_rgSettings[nIdx] = value; }
        }

        public void Add(DataConfigSetting s)
        {
            m_rgSettings.Add(s);
        }

        public bool Remove(DataConfigSetting s)
        {
            return m_rgSettings.Remove(s);
        }

        public void RemoveAt(int nIdx)
        {
            m_rgSettings.RemoveAt(nIdx);
        }

        public void Clear()
        {
            m_rgSettings.Clear();
        }

        public static DataConfigSetting Find(DataConfigSetting[] rgSettings, string strName)
        {
            foreach (DataConfigSetting s in rgSettings)
            {
                if (s.Name == strName)
                    return s;
            }

            return null;
        }

        public DataConfigSetting Find(string strName)
        {
            return DataConfigSettingCollection.Find(m_rgSettings.ToArray(), strName);
        }

        public DataConfigSettingCollection Clone()
        {
            DataConfigSettingCollection col = new DataConfigSettingCollection();

            foreach (DataConfigSetting s in m_rgSettings)
            {
                col.Add(s.Clone());
            }

            return col;
        }

        public IEnumerator<DataConfigSetting> GetEnumerator()
        {
            return m_rgSettings.GetEnumerator();
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return m_rgSettings.GetEnumerator();
        }
    }

    [Serializable]
    [EditorAttribute(typeof(DataConfigSettingEditor), typeof(System.Drawing.Design.UITypeEditor))]
    [ReadOnly(false)]
    public class DataConfigSetting
    {
        string m_strName = "";
        object m_objValue = null;
        TYPE m_type = TYPE.TEXT;
        string m_strExtra = "";

        [NonSerialized]
        IXDatasetCreatorSettings m_iverify = null;


        public enum TYPE
        {
            TEXT,
            FILENAME,
            FILENAME1,
            DIRECTORY,
            LIST,
            DATETIME,
            INTEGER,
            REAL,
            CUSTOM,
            HELP
        }

        public DataConfigSetting(string strName = "", object objValue = null, TYPE type = TYPE.TEXT, string strExtra = "", IXDatasetCreatorSettings iverify = null) 
        {
            m_strName = strName;
            m_objValue = objValue;
            m_strExtra = strExtra;
            m_type = type;
            m_iverify = iverify;
        }

        [Browsable(false)]
        public IXDatasetCreatorSettings VerifyInterface
        {
            get { return m_iverify; }
        }

        public string Name
        {
            get { return m_strName; }
        }

        [Browsable(false)]
        public string Extra
        {
            get { return m_strExtra; }
            set { m_strExtra = value; }
        }
        
        [ReadOnly(false)]
        public object Value
        {
            get { return m_objValue; }
            set { m_objValue = value; }
        }

        public TYPE Type
        {
            get { return m_type; }
        }

        public DataConfigSetting Clone()
        {
            return new DataConfigSetting(m_strName, m_objValue, m_type, m_strExtra, m_iverify);
        }

        public override string ToString()
        {
            return m_strName + ": " + m_objValue.ToString();
        }

        public string ToSaveString()
        {
            string strType = (m_type == TYPE.LIST) ? "TEXT" : m_type.ToString();
            return m_strName + "; " + m_objValue.ToString() + "; " + strType + "; " + m_strExtra;
        }

        public static DataConfigSetting Parse(string str, IXDatasetCreatorSettings iVerify = null)
        {
            string[] rgstr = str.Split(';');

            if (rgstr.Length < 3)
                throw new Exception("Invalid setting '" + str + "'!");

            string strName = rgstr[0].Trim();
            string strVal = rgstr[1].Trim();
            string strType = rgstr[2].Trim();
            string strExtra = rgstr[3].Trim();
            object objVal = getValue(strVal, strType, out TYPE type);

            return new DataConfigSetting(strName, objVal, type, strExtra, iVerify);
        }

        private static object getValue(string strVal, string strType, out TYPE type)
        {
            if (strType == TYPE.INTEGER.ToString())
            {
                type = TYPE.INTEGER;
                return int.Parse(strVal);
            }
            else if (strType == TYPE.REAL.ToString())
            {
                type = TYPE.REAL;
                return double.Parse(strVal);
            }
            else if (strType == TYPE.DATETIME.ToString())
            {
                type = TYPE.DATETIME;
                return DateTime.Parse(strVal);
            }
            else if (strType == TYPE.DIRECTORY.ToString())
            {
                type = TYPE.DIRECTORY;
                return strVal;
            }
            else if (strType == TYPE.FILENAME.ToString())
            {
                type = TYPE.FILENAME;
                return strVal;
            }
            else if (strType == TYPE.FILENAME1.ToString())
            {
                type = TYPE.FILENAME1;
                return strVal;
            }
            else if (strType == TYPE.HELP.ToString())
            { 
                type = TYPE.HELP;
                return strVal;
            }
            else if (strType == TYPE.CUSTOM.ToString())
            {
                type = TYPE.CUSTOM;
                return strVal;
            }
            else if (strType == TYPE.LIST.ToString())
            {
                type = TYPE.LIST;
                return getList(strVal);
            }
            else
            {
                type = TYPE.TEXT;
                return strVal;
            }
        }

        private static object getList(string str)
        {
            string[] rgstr = str.Split(',');
            OptionItemList options = new OptionItemList();

            for (int i = 0; i < rgstr.Length; i++)
            {
                options.Add(new OptionItem(rgstr[i], i));
            }

            return options;
        }
    }

    public class OptionItem
    {
        string m_strName = "";
        int m_nIdx = 0;
        OptionItemList m_rgOptionItemList = new OptionItemList();

        public OptionItem(string strName, int nIdx, OptionItemList items = null)
        {
            m_strName = strName;
            m_nIdx = nIdx;

            if (items != null)
                m_rgOptionItemList = items;
        }

        public string Name
        {
            get { return m_strName; }
        }

        public int Index
        {
            get { return m_nIdx; }
            set { m_nIdx = value; }
        }

        public OptionItemList Options
        {
            get { return m_rgOptionItemList; }
            set { m_rgOptionItemList = value; }
        }

        public OptionItem Clone()
        {
            OptionItem item = new OptionItem(m_strName, m_nIdx, Options);
            return item;
        }

        public override string ToString()
        {
            return m_strName;
        }
    }

    public class OptionItemList : IEnumerable<OptionItem>
    {
        List<OptionItem> m_rgItems = new List<OptionItem>();

        public OptionItemList()
        {
        }

        public int Count
        {
            get { return m_rgItems.Count; }
        }

        public OptionItem this[int nIdx]
        {
            get { return m_rgItems[nIdx]; }
        }

        public OptionItem Find(string strItem)
        {
            foreach (OptionItem item in m_rgItems)
            {
                if (item.Name == strItem)
                    return item;
            }

            return null;
        }

        public OptionItemList Clone()
        {
            OptionItemList list = new OptionItemList();

            foreach (OptionItem item in m_rgItems)
            {
                list.Add(item);
            }

            return list;
        }

        public void Add(OptionItem item)
        {
            m_rgItems.Add(item);
        }

        public void Clear()
        {
            m_rgItems.Clear();
        }

        public IEnumerator<OptionItem> GetEnumerator()
        {
            return m_rgItems.GetEnumerator();
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return m_rgItems.GetEnumerator();
        }
    }

    public class DataConfigSettingEditor : UITypeEditor
    {
        public DataConfigSettingEditor()
            : base()
        {
        }

        public override UITypeEditorEditStyle GetEditStyle(ITypeDescriptorContext context)
        {
            if (context != null && context.PropertyDescriptor != null)
            {
                int nIdx = getIndex(context.PropertyDescriptor.Name);
                DataConfigSetting[] config = context.Instance as DataConfigSetting[];
                
                if (config[nIdx].Type == DataConfigSetting.TYPE.FILENAME)
                    return UITypeEditorEditStyle.Modal;
                if (config[nIdx].Type == DataConfigSetting.TYPE.FILENAME1)
                    return UITypeEditorEditStyle.Modal;
                else if (config[nIdx].Type == DataConfigSetting.TYPE.DIRECTORY)
                    return UITypeEditorEditStyle.Modal;
                else if (config[nIdx].Type == DataConfigSetting.TYPE.CUSTOM)
                    return UITypeEditorEditStyle.Modal;
                else if (config[nIdx].Type == DataConfigSetting.TYPE.LIST)
                    return UITypeEditorEditStyle.DropDown;
                else if (config[nIdx].Type == DataConfigSetting.TYPE.DATETIME)
                    return UITypeEditorEditStyle.Modal;
                else if (config[nIdx].Type == DataConfigSetting.TYPE.INTEGER)
                    return UITypeEditorEditStyle.DropDown;
                else if (config[nIdx].Type == DataConfigSetting.TYPE.REAL)
                    return UITypeEditorEditStyle.DropDown;
                else if (config[nIdx].Type == DataConfigSetting.TYPE.TEXT)
                    return UITypeEditorEditStyle.DropDown;
            }

            return UITypeEditorEditStyle.None;
        }

        private int getIndex(string strName)
        {
            strName = strName.Trim('[', ']');
            return int.Parse(strName);
        }

        public override object EditValue(ITypeDescriptorContext context, IServiceProvider provider, object value)
        {
            DataConfigSetting setting = value as DataConfigSetting;
            IWindowsFormsEditorService edSvc = (IWindowsFormsEditorService)provider.GetService(typeof(IWindowsFormsEditorService));

            if (setting.Type == DataConfigSetting.TYPE.FILENAME ||
                setting.Type == DataConfigSetting.TYPE.FILENAME1)
            {
                OpenFileDialog dlg = new OpenFileDialog();

                dlg.Filter = "Data Files (*." + setting.Extra + ")|*." + setting.Extra + "||";
                dlg.Title = "Select the " + setting.Name;
                dlg.DefaultExt = setting.Extra;
                dlg.FileName = (string)setting.Value;

                if (setting.Type == DataConfigSetting.TYPE.FILENAME1)
                    dlg.CheckFileExists = false;

                if (dlg.ShowDialog() == DialogResult.OK)
                    setting.Value = dlg.FileName;
                else
                    setting.Value = "";
            }
            else if (setting.Type == DataConfigSetting.TYPE.DIRECTORY)
            {
                FolderBrowserDialog dlg = new FolderBrowserDialog();

                dlg.RootFolder = Environment.SpecialFolder.MyComputer;
                dlg.SelectedPath = setting.Value.ToString();
                dlg.ShowNewFolderButton = true;

                if (dlg.ShowDialog() == DialogResult.OK)
                    setting.Value = dlg.SelectedPath;
            }
            else if (setting.Type == DataConfigSetting.TYPE.LIST)
            {
                if (edSvc != null)
                {
                    ListBox list = new ListBox();

                    list.SelectedIndexChanged += new EventHandler(list_SelectedIndexChanged);

                    OptionItem item = setting.Value as OptionItem;

                    foreach (OptionItem option in item.Options)
                    {
                        list.Items.Add(option.Name);
                    }

                    list.Tag = edSvc;
                    edSvc.DropDownControl(list);

                    if (list.SelectedItem != null)
                    {
                        OptionItem selectedItem = item.Options.Find(list.SelectedItem.ToString());
                        selectedItem.Options = item.Options;

                        setting.Value = selectedItem;
                    }
                }
            }
            else if (setting.Type == DataConfigSetting.TYPE.DATETIME)
            {
                if (edSvc != null)
                {
                    DateTime dt = DateTime.Parse(setting.Value.ToString());

                    FormDateTime dlg = new FormDateTime(dt);
                    if (dlg.ShowDialog() == DialogResult.OK)
                        setting.Value = dlg.SelectedDateTime.ToString();
                }
            }
            else if (setting.Type == DataConfigSetting.TYPE.CUSTOM)
            {
                if (setting.VerifyInterface != null)
                {
                    setting.VerifyInterface.GetCustomSetting(setting.Name, setting.Extra, (DataConfigSetting[])context.Instance);
                }
            }
            else
            {
                if (edSvc != null)
                {
                    TextBox edt = new TextBox();

                    edt.Text = setting.Value.ToString();
                    edt.Tag = edSvc;

                    edSvc.DropDownControl(edt);

                    if (setting.Type == DataConfigSetting.TYPE.INTEGER)
                    {
                        int nVal;

                        if (!int.TryParse(edt.Text, out nVal))
                            throw new Exception("The value specified for '" + setting.Name + "' is invalid.  Please enter a valid INTEGER number.");
                    }
                    else if (setting.Type == DataConfigSetting.TYPE.REAL)
                    {
                        double dfVal;

                        if (!double.TryParse(edt.Text, out dfVal))
                            throw new Exception("The value specified for '" + setting.Name + "' is invalid.  Please enter a valid REAL number.");
                    }

                    setting.Value = edt.Text;
                }
            }

            if (setting.VerifyInterface != null)
                setting.VerifyInterface.VerifyConfiguration((DataConfigSetting[])context.Instance);

            return value;
        }

        void list_SelectedIndexChanged(object sender, EventArgs e)
        {
            ListBox list = sender as ListBox;
            IWindowsFormsEditorService edSvc = list.Tag as IWindowsFormsEditorService;

            edSvc.CloseDropDown();
        }

        void calendar_DateSelected(object sender, DateRangeEventArgs e)
        {
            MonthCalendar calendar = sender as MonthCalendar;
            IWindowsFormsEditorService edSvc = calendar.Tag as IWindowsFormsEditorService;

            edSvc.CloseDropDown();
        }
    }
}
