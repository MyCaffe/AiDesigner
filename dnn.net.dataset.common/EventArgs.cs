using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DNN.net.dataset.common
{
    public class CreateProgressArgs : EventArgs
    {
        bool m_bAbort = false;
        bool m_bAborted = false;
        double m_dfPercentComplete = 0;
        string m_strMessage = "";
        Exception m_error;

        public CreateProgressArgs(int nIdx, int nCount, string strMsg, Exception err = null, bool bAborted = false)
            : base()
        {
            if (nCount != 0)
                m_dfPercentComplete = (double)nIdx / (double)nCount;

            m_strMessage = strMsg;
            m_error = err;
            m_bAborted = bAborted;
        }

        public CreateProgressArgs(double dfProgress, string strMsg, Exception err = null, bool bAborted = false)
            : base()
        {
            m_dfPercentComplete = dfProgress;
            m_strMessage = strMsg;
            m_error = err;
            m_bAborted = bAborted;
        }

        public bool Aborted
        {
            get { return m_bAborted; }
        }

        public double PercentComplete
        {
            get { return m_dfPercentComplete; }
        }

        public string PercentCompleteAsText
        {
            get { return PercentComplete.ToString("P"); }
        }

        public string Message
        {
            get { return m_strMessage; }
        }

        public Exception Error
        {
            get { return m_error; }
        }

        public bool Abort
        {
            get { return m_bAbort; }
            set { m_bAbort = value; }
        }
    }
}
