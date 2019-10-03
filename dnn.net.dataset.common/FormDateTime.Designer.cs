namespace DNN.net.dataset.common
{
    partial class FormDateTime
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.dtpDate = new System.Windows.Forms.MonthCalendar();
            this.lblDate = new System.Windows.Forms.Label();
            this.lblTime = new System.Windows.Forms.Label();
            this.btnOK = new System.Windows.Forms.Button();
            this.btnCancel = new System.Windows.Forms.Button();
            this.edtTime = new System.Windows.Forms.MaskedTextBox();
            this.radUtc = new System.Windows.Forms.RadioButton();
            this.radLocal = new System.Windows.Forms.RadioButton();
            this.SuspendLayout();
            // 
            // dtpDate
            // 
            this.dtpDate.Location = new System.Drawing.Point(15, 31);
            this.dtpDate.MaxSelectionCount = 1;
            this.dtpDate.Name = "dtpDate";
            this.dtpDate.TabIndex = 1;
            // 
            // lblDate
            // 
            this.lblDate.AutoSize = true;
            this.lblDate.Location = new System.Drawing.Point(12, 9);
            this.lblDate.Name = "lblDate";
            this.lblDate.Size = new System.Drawing.Size(33, 13);
            this.lblDate.TabIndex = 0;
            this.lblDate.Text = "Date:";
            // 
            // lblTime
            // 
            this.lblTime.AutoSize = true;
            this.lblTime.Location = new System.Drawing.Point(12, 202);
            this.lblTime.Name = "lblTime";
            this.lblTime.Size = new System.Drawing.Size(33, 13);
            this.lblTime.TabIndex = 2;
            this.lblTime.Text = "Time:";
            // 
            // btnOK
            // 
            this.btnOK.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Right)));
            this.btnOK.DialogResult = System.Windows.Forms.DialogResult.OK;
            this.btnOK.Location = new System.Drawing.Point(88, 237);
            this.btnOK.Name = "btnOK";
            this.btnOK.Size = new System.Drawing.Size(75, 23);
            this.btnOK.TabIndex = 6;
            this.btnOK.Text = "OK";
            this.btnOK.UseVisualStyleBackColor = true;
            this.btnOK.Click += new System.EventHandler(this.btnOK_Click);
            // 
            // btnCancel
            // 
            this.btnCancel.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Right)));
            this.btnCancel.DialogResult = System.Windows.Forms.DialogResult.Cancel;
            this.btnCancel.Location = new System.Drawing.Point(169, 237);
            this.btnCancel.Name = "btnCancel";
            this.btnCancel.Size = new System.Drawing.Size(75, 23);
            this.btnCancel.TabIndex = 7;
            this.btnCancel.Text = "Cancel";
            this.btnCancel.UseVisualStyleBackColor = true;
            // 
            // edtTime
            // 
            this.edtTime.Location = new System.Drawing.Point(51, 199);
            this.edtTime.Mask = "90:00";
            this.edtTime.Name = "edtTime";
            this.edtTime.Size = new System.Drawing.Size(40, 20);
            this.edtTime.TabIndex = 3;
            this.edtTime.Text = "0630";
            this.edtTime.ValidatingType = typeof(System.DateTime);
            // 
            // radUtc
            // 
            this.radUtc.AutoSize = true;
            this.radUtc.Location = new System.Drawing.Point(106, 200);
            this.radUtc.Name = "radUtc";
            this.radUtc.Size = new System.Drawing.Size(47, 17);
            this.radUtc.TabIndex = 4;
            this.radUtc.Text = "UTC";
            this.radUtc.UseVisualStyleBackColor = true;
            // 
            // radLocal
            // 
            this.radLocal.AutoSize = true;
            this.radLocal.Checked = true;
            this.radLocal.Location = new System.Drawing.Point(159, 200);
            this.radLocal.Name = "radLocal";
            this.radLocal.Size = new System.Drawing.Size(51, 17);
            this.radLocal.TabIndex = 5;
            this.radLocal.TabStop = true;
            this.radLocal.Text = "Local";
            this.radLocal.UseVisualStyleBackColor = true;
            // 
            // FormDateTime
            // 
            this.AcceptButton = this.btnOK;
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.CancelButton = this.btnCancel;
            this.ClientSize = new System.Drawing.Size(256, 272);
            this.Controls.Add(this.radLocal);
            this.Controls.Add(this.radUtc);
            this.Controls.Add(this.edtTime);
            this.Controls.Add(this.btnCancel);
            this.Controls.Add(this.btnOK);
            this.Controls.Add(this.lblTime);
            this.Controls.Add(this.lblDate);
            this.Controls.Add(this.dtpDate);
            this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.FixedDialog;
            this.MaximizeBox = false;
            this.MinimizeBox = false;
            this.Name = "FormDateTime";
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterParent;
            this.Text = "Date/Time";
            this.Load += new System.EventHandler(this.FormDateTime_Load);
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.MonthCalendar dtpDate;
        private System.Windows.Forms.Label lblDate;
        private System.Windows.Forms.Label lblTime;
        private System.Windows.Forms.Button btnOK;
        private System.Windows.Forms.Button btnCancel;
        private System.Windows.Forms.MaskedTextBox edtTime;
        private System.Windows.Forms.RadioButton radUtc;
        private System.Windows.Forms.RadioButton radLocal;
    }
}