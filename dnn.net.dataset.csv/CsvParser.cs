using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DNN.net.dataset.csv
{
    public class CsvParser
    {
        List<string> m_rgDataDescriptions = new List<string>();
        List<string> m_rgExtraDescriptions = new List<string>();
        List<string> m_rgLabels = new List<string>();
        List<DataItem> m_rgData = new List<DataItem>();


        public CsvParser()
        {
        }

        public List<string> DataDescriptions
        {
            get { return m_rgDataDescriptions; }
        }

        public List<string> ExtraDescriptions
        {
            get { return m_rgExtraDescriptions; }
        }

        public List<string> Labels
        {
            get { return m_rgLabels; }
        }

        public List<DataItem> Data
        {
            get { return m_rgData; }
        }

        public void Load(string strFile, Schema schema)
        {
            List<string> rgstrLines = new List<string>();

            using (StreamReader sr = new StreamReader(strFile))
            {
                string strLine = sr.ReadLine();

                while (strLine != null)
                {
                    rgstrLines.Add(strLine);
                    strLine = sr.ReadLine();
                }
            }

            m_rgDataDescriptions = loadDescriptions(rgstrLines, schema.Col.DataStart, schema.Col.DataEnd, schema.Row.DataDescription, schema.Row.DataDescription);
            m_rgExtraDescriptions = loadDescriptions(rgstrLines, schema.Col.ExtraStart, schema.Col.ExtraEnd, schema.Row.LabelStart, schema.Row.LabelEnd);
            m_rgLabels = loadDescriptions(rgstrLines, schema.Col.LabelStart, schema.Col.LabelEnd, schema.Row.LabelStart, schema.Row.LabelEnd);
            m_rgData = new List<DataItem>();

            for (int i = schema.Row.DataStart; i < rgstrLines.Count; i++)
            {
                string[] rgstr = rgstrLines[i].Split(',');
                if (rgstr.Length < 1 || rgstr[0] == "***" || rgstr[0].Length == 0)
                    break;

                m_rgData.Add(new DataItem(i, rgstrLines[i], schema.Col));
            }
        }

        private List<string> loadDescriptions(List<string> rgstrLines, int nColStart, int nColEnd, int nRowStart, int nRowEnd)
        {
            List<List<string>> rgrgGrid = new List<List<string>>();

            if (nRowStart < 0 || nRowEnd < nRowStart || nRowEnd > rgstrLines.Count || nRowStart > rgstrLines.Count)
                throw new Exception("Invalid row values!");

            for (int i = nRowStart; i <= nRowEnd; i++)
            {
                string[] rgstr = rgstrLines[i].Split(',');

                if (nColStart < 0 || nColEnd < nColStart || nColEnd > rgstr.Length || nColStart > rgstr.Length)
                    throw new Exception("Invalid col values!");

                List<string> rgstr1 = new List<string>();

                for (int j = nColStart; j <= nColEnd; j++)
                {
                    rgstr1.Add(rgstr[j]);
                }

                rgrgGrid.Add(rgstr1);
            }

            if (rgrgGrid.Count == 1)
                return rgrgGrid[0];

            List<string> rgstrVal = new List<string>();

            for (int i = 0; i < rgrgGrid[0].Count; i++)
            {
                string strVal = "";

                for (int j = 0; j < rgrgGrid.Count; j++)
                {
                    string strItem = rgrgGrid[j][i];

                    strItem = strItem.Trim();

                    if (strItem.Length > 0)
                    {
                        strVal += rgrgGrid[j][i];
                        strVal += " ";
                    }
                }

                strVal = strVal.Trim();
                rgstrVal.Add(strVal);
            }

            return rgstrVal;
        }
    }

    public class SchemaColumn
    {
        int m_nTagCol = -1;
        int m_nDateCol = -1;
        int m_nDataStartCol = 0;
        int m_nDataEndCol = 0;
        int m_nLabelStartCol = 0;
        int m_nLabelEndCol = 0;
        int m_nExtraStartCol = 0;
        int m_nExtraEndCol = 0;

        public SchemaColumn(int nTag, int nDate, int nDataStart, int nDataEnd, int nLabelStart, int nLabelEnd, int nExtraStart, int nExtraEnd)
        {
            m_nTagCol = nTag;
            m_nDateCol = nDate;
            m_nDataStartCol = nDataStart;
            m_nDataEndCol = nDataEnd;
            m_nLabelStartCol = nLabelStart;
            m_nLabelEndCol = nLabelEnd;
            m_nExtraStartCol = nExtraStart;
            m_nExtraEndCol = nExtraEnd;
        }

        public int Tag
        {
            get { return m_nTagCol; }
        }

        public int Date
        {
            get { return m_nDateCol; }
        }

        public int DataStart
        {
            get { return m_nDataStartCol; }
        }

        public int DataEnd
        {
            get { return m_nDataEndCol; }
        }

        public int LabelStart
        {
            get { return m_nLabelStartCol; }
        }

        public int LabelEnd
        {
            get { return m_nLabelEndCol; }
        }

        public int ExtraStart
        {
            get { return m_nExtraStartCol; }
        }

        public int ExtraEnd
        {
            get { return m_nExtraEndCol; }
        }

        public static int ConvertCol(string str)
        {
            int nCol = 0;

            str = str.Trim();

            if (str.Length == 0)
                return -1;

            str = str.ToUpper();

            for (int i = 0; i < str.Length; i++)
            {
                int nVal = str[i] - (int)'A';
                nCol += (i * 26) + nVal;
            }

            return nCol;
        }
    }

    public class SchemaRow
    {
        int m_nLabelStartRow = 0;
        int m_nLabelEndRow = 0;
        int m_nDataStartRow = 0;
        int m_nDataDescriptionRow = 0;

        public SchemaRow(int nDataDescription, int nLabelStart, int nLabelEnd, int nDataStart)
        {
            m_nDataDescriptionRow = nDataDescription;
            m_nLabelStartRow = nLabelStart;
            m_nLabelEndRow = nLabelEnd;
            m_nDataStartRow = nDataStart;
        }

        public int LabelStart
        {
            get { return m_nLabelStartRow; }
        }

        public int LabelEnd
        {
            get { return m_nLabelEndRow; }
        }

        public int DataStart
        {
            get { return m_nDataStartRow; }
        }

        public int DataDescription
        {
            get { return m_nDataDescriptionRow; }
        }
    }

    public class Schema
    {
        SchemaColumn m_col;
        SchemaRow m_row;
        bool m_bBinaryValues = true;
        int m_nCellSize = 5;

        public Schema(SchemaColumn col, SchemaRow row, bool bBinaryValues, int nCellSize)
        {
            m_col = col;
            m_row = row;
            m_bBinaryValues = bBinaryValues;
            m_nCellSize = nCellSize;
        }

        public SchemaColumn Col
        {
            get { return m_col; }
        }

        public SchemaRow Row
        {
            get { return m_row; }
        }

        public bool BinaryValues
        {
            get { return m_bBinaryValues; }
        }

        public int CellSize
        {
            get { return m_nCellSize; }
        }
    }

    public class DataItem
    {
        int m_nLabel;
        string m_strTag;
        DateTime m_dt;
        List<double> m_rgData = new List<double>();
        List<double> m_rgExtra = new List<double>();

        public DataItem(int nLine, string str, SchemaColumn col)
        {
            string[] rgstr = str.Split(',');

            if (col.Tag >= 0)
                m_strTag = rgstr[col.Tag].Trim();

            if (col.Date >= 0)
                m_dt = DateTime.Parse(rgstr[col.Date].Trim());

            if (m_dt.DayOfWeek == DayOfWeek.Saturday || m_dt.DayOfWeek == DayOfWeek.Sunday)
            {
                DateTime dt = m_dt;

                while (dt.DayOfWeek != DayOfWeek.Monday)
                {
                    dt += TimeSpan.FromDays(1);
                }

                Trace.WriteLine("Weekend date on line " + (nLine + 1).ToString() + ", use " + dt.ToShortDateString() + " instead.");
            }

            if (rgstr.Length < col.DataStart)
                throw new Exception("LINE (" + (nLine + 1).ToString() + "): The data start '" + col.DataStart.ToString() + "' exceeds the row length of '" + rgstr.Length.ToString() + "!");

            if (rgstr.Length < col.DataEnd)
                throw new Exception("LINE (" + (nLine + 1).ToString() + "): The data end '" + col.DataEnd.ToString() + "' exceeds the row length of '" + rgstr.Length.ToString() + "!");

            if (col.DataStart < 0 || col.DataEnd < 0 || col.DataStart > col.DataEnd)
                throw new Exception("LINE (" + (nLine + 1).ToString() + "): The data start and end '" + col.DataStart.ToString() + ", " + col.DataEnd.ToString() + "' are out of range!");

            for (int i = col.DataStart; i <= col.DataEnd; i++)
            {
                string strData = rgstr[i].Trim();

                if (string.IsNullOrEmpty(strData))
                    m_rgData.Add(0);
                else
                    m_rgData.Add(double.Parse(strData));
            }

            if (rgstr.Length < col.LabelStart)
                throw new Exception("LINE (" + (nLine + 1).ToString() + "): The label start '" + col.LabelStart.ToString() + "' exceeds the row length of '" + rgstr.Length.ToString() + "!");

            if (rgstr.Length < col.LabelEnd)
                throw new Exception("LINE (" + (nLine + 1).ToString() + "): The label end '" + col.LabelEnd.ToString() + "' exceeds the row length of '" + rgstr.Length.ToString() + "!");

            if (col.LabelStart < 0 || col.LabelEnd < 0 || col.LabelStart > col.LabelEnd)
                throw new Exception("LINE (" + (nLine + 1).ToString() + "): The label start and end '" + col.LabelStart.ToString() + ", " + col.LabelEnd.ToString() + "' are out of range!");

            m_nLabel = -1;

            for (int i = col.LabelStart; i <= col.LabelEnd; i++)
            {
                string strData = rgstr[i].Trim();

                if (!string.IsNullOrEmpty(strData))
                {
                    m_nLabel = i - col.LabelStart;
                    break;
                }
            }

            if (m_nLabel < 0)
                throw new Exception("LINE (" + (nLine + 1).ToString() + "): Missing label specification!");

            if (rgstr.Length < col.ExtraStart)
                throw new Exception("LINE (" + (nLine + 1).ToString() + "): The extra start '" + col.ExtraStart.ToString() + "' exceeds the row length of '" + rgstr.Length.ToString() + "!");

            if (rgstr.Length < col.ExtraEnd)
                throw new Exception("LINE (" + (nLine + 1).ToString() + "): The extra end '" + col.ExtraEnd.ToString() + "' exceeds the row length of '" + rgstr.Length.ToString() + "!");

            if (col.ExtraStart >= 0 && col.ExtraEnd > col.ExtraStart)
            {
                for (int i = col.ExtraStart; i <= col.ExtraEnd; i++)
                {
                    string strData = rgstr[i].Trim();

                    strData = strData.TrimEnd('%');

                    if (string.IsNullOrEmpty(strData))
                        m_rgExtra.Add(0);
                    else
                        m_rgExtra.Add(double.Parse(strData));
                }
            }
        }

        public int Label
        {
            get { return m_nLabel; }
        }

        public string Tag
        {
            get { return m_strTag; }
        }

        public DateTime TimeStamp
        {
            get { return m_dt; }
        }

        public List<double> Data
        {
            get { return m_rgData; }
        }

        public List<double> Extra
        {
            get { return m_rgExtra; }
        }
    }
}
