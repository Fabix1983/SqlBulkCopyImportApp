using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Configuration;
using System.Security.Policy;
using System.Text;

namespace SqlBulkCopyImportApp
{
    internal class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("START");

            string myConnString = ConfigurationManager.ConnectionStrings["myConnString"].ConnectionString;
            var Exec_Delete = false;

            string fileName = Path.GetFullPath(Directory.GetCurrentDirectory() + "/listacomuni.csv");
            fileName = fileName.Replace("\\bin\\Debug", "");

            DataTable ListaComuni = ConvertCSVtoDataTable(fileName, ';');

            using (SqlConnection con = new SqlConnection(myConnString))
            {
                try
                {
                    con.Open();
                    SqlCommand cmd = new SqlCommand("DELETE FROM DbTest.dbo.tblComuni", con);
                    int RecordCancellati;
                    RecordCancellati = cmd.ExecuteNonQuery();
                    Console.WriteLine("Righe Eliminate: " + RecordCancellati.ToString());
                    Exec_Delete = true;
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Errore catch DELETE " + ex.Message.ToString());
                }
            }

            if (Exec_Delete)
            {
                Stopwatch watch = new Stopwatch();
                watch.Start();

                using (SqlConnection connection = new SqlConnection(myConnString))
                {
                    connection.Open();
                    try
                    {
                        using (SqlBulkCopy sbc = new SqlBulkCopy(connection))
                        {
                            sbc.DestinationTableName = "DbTest.dbo.tblComuni";

                            // column mappings if necessary
                            sbc.ColumnMappings.Add("Comune", "Comune");
                            sbc.ColumnMappings.Add("Provincia", "Provincia");
                            sbc.ColumnMappings.Add("Regione", "Regione");
                            sbc.ColumnMappings.Add("CAP", "CAP");
                            sbc.ColumnMappings.Add("Abitanti", "Abitanti");

                            try
                            {
                                sbc.WriteToServer(ListaComuni);

                                watch.Stop();
                                Console.WriteLine("Db insert execution Time: " + watch.ElapsedMilliseconds.ToString());

                                string countRow = "";

                                SqlCommand commandRowCount = new SqlCommand("SELECT COUNT(*) FROM DbTest.dbo.tblComuni", connection);

                                try
                                {
                                    countRow = commandRowCount.ExecuteScalar().ToString();
                                    Console.WriteLine("Conteggio Record Insert: " + countRow.ToString());

                                    if (countRow == ListaComuni.Rows.Count.ToString())
                                    {
                                        Console.WriteLine("Controll record number file and db insert: OK (db insert:" + countRow + " file:" + ListaComuni.Rows.Count.ToString() + ")");               
                                    }
                                    else
                                    {
                                        Console.WriteLine("Controll record number file and db insert: FAILED (db insert:" + countRow + " file:" + ListaComuni.Rows.Count.ToString() + ")" );
                                    }
                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine("Error Catch Conteggio db Insert: " + ex.ToString());
                                }
                            }
                            catch (SqlException ex)
                            {
                                Console.WriteLine("Error Catch SQLBULKCOPY: " + ex.ToString());
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("Error Catch Established Connection for SQLBULKCOPY: " + ex.ToString());
                    }
                }
            }
            else
            {
                Console.WriteLine("Delete Execution Failed" );
            }

            Console.WriteLine("END");
            Console.ReadLine();
        }

        public static DataTable ConvertCSVtoDataTable(string strFilePath, char ColumnSeparatorChart)
        {
            DataTable dt = new DataTable();

            // Uso Encoding.Default per la codifica dei caratteri speciali

            using (StreamReader sr = new StreamReader(strFilePath, Encoding.Default))
            {
                string[] headers = sr.ReadLine().Split(ColumnSeparatorChart);
                foreach (string header in headers)
                {
                    dt.Columns.Add(header);
                }
                while (!sr.EndOfStream)
                {
                    string[] rows = sr.ReadLine().Split(ColumnSeparatorChart);
                    DataRow dr = dt.NewRow();
                    for (int i = 0; i < headers.Length; i++)
                    {
                        dr[i] = rows[i];
                    }
                    dt.Rows.Add(dr);
                }
            }
            return dt;
        }

        /*
         CREATE TABLE [dbo].[tblComuni](
	        [Comune] [varchar](150) NULL,
	        [Provincia] [varchar](150) NULL,
	        [Regione] [varchar](150) NULL,
	        [CAP] [varchar](150) NULL,
	        [Abitanti] [varchar](150) NULL
        ) ON [PRIMARY]
        GO    
        */
    }
}
