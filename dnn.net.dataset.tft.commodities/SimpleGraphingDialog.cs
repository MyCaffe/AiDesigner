using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using SimpleGraphing;

namespace SimpleGraphing
{
    public partial class SimpleGraphingDialog : Form
    {
        public SimpleGraphingDialog()
        {
            InitializeComponent();
        }

        private void SimpleGraphingDialog_Load(object sender, EventArgs e)
        {
            PlotCollection plots = new PlotCollection();

            for (int i = 0; i < 200; i++)
            {
                double dfY = Math.Sin(i/10.0);
                double dfX = i;

                plots.Add(dfX, dfY);
            }

            pictureBox1.Image = SimpleGraphingControl.QuickRender(plots, pictureBox1.Width, pictureBox1.Height);
        }
    }
}
