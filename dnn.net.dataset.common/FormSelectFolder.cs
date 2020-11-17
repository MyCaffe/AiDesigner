using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace DNN.net.dataset.common
{
    public partial class FormSelectFolder : Form
    {
        string m_strSelectedFolder = "";

        public FormSelectFolder(string strFolder)
        {
            InitializeComponent();
            m_strSelectedFolder = strFolder;
        }

        public string SelectedFolder
        {
            get { return m_strSelectedFolder; }
        }

        private void FormSelectFolder_Load(object sender, EventArgs e)
        {
            edtFolder.Text = m_strSelectedFolder;
        }

        private void btnBrowse_Click(object sender, EventArgs e)
        {
            folderBrowserDialog1.SelectedPath = edtFolder.Text;

            if (folderBrowserDialog1.ShowDialog() == DialogResult.OK)
                edtFolder.Text = folderBrowserDialog1.SelectedPath;
        }

        private void timerUI_Tick(object sender, EventArgs e)
        {
            if (Directory.Exists(edtFolder.Text))
                btnOK.Enabled = true;
            else
                btnOK.Enabled = false;
        }

        private void btnOK_Click(object sender, EventArgs e)
        {
            m_strSelectedFolder = edtFolder.Text;
        }
    }
}
