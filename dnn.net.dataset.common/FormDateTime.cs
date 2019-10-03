using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace DNN.net.dataset.common
{
    public partial class FormDateTime : Form
    {
        DateTime m_dt;

        public FormDateTime(DateTime dt)
        {
            InitializeComponent();
            m_dt = dt;
        }

        private void FormDateTime_Load(object sender, EventArgs e)
        {
            DateTime dt = new DateTime(m_dt.Year, m_dt.Month, m_dt.Day);
            dtpDate.SetSelectionRange(dt, dt);

            string strTime = dt.Hour.ToString("00");
            strTime += dt.Minute.ToString("00");

            edtTime.Text = strTime;

            radUtc.Checked = (m_dt.Kind == DateTimeKind.Utc) ? true : false;
        }

        private void btnOK_Click(object sender, EventArgs e)
        {
            string strTime = edtTime.Text;
            string[] rgstr = strTime.Split(':');

            string strHour = rgstr[0];
            string strMin = rgstr[1];

            int nHour = int.Parse(strHour);
            int nMin = int.Parse(strMin);

            DateTimeKind kind = (radUtc.Checked) ? DateTimeKind.Utc : DateTimeKind.Local;
            DateTime dt = dtpDate.SelectionStart;
            m_dt = new DateTime(dt.Year, dt.Month, dt.Day, nHour, nMin, 0, kind);
        }

        public DateTime SelectedDateTime
        {
            get { return m_dt; }
        }
    }
}
