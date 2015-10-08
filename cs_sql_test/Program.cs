﻿/*
 * Author: Garrett Bates
 * Date: October 6, 2015
 * Program: Programmatic MQL Demo
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Net;
using System.Xml;
using System.IO;
using System.Timers;
using System.Threading;

using MySql.Data.MySqlClient;

namespace cs_sql_test
{
    class Program
    {
        static DateTime _cachedUntilUTC;
        static DateTime _documentTimeUTC;
        static System.Timers.Timer _timer = new System.Timers.Timer();
        static int _seconds_of_delay = 10;

        static void Main(string[] args)
        {
            _timer.Elapsed += new ElapsedEventHandler(_timer_Elapsed);
            _timer.Interval = 1000;
            _timer.Enabled = true;

            while (true)
            {
                Thread.Sleep(10);
            }
        }


        static void _timer_Elapsed(object sender, ElapsedEventArgs e)
        {
            Console.Clear();
            _timer.Enabled = false;
            DoWork();
        }

        private static void DoWork()
        {
            string data_time = null;
            const string connStr = "SERVER=localhost;" +
                 "DATABASE=evesde;" +
                 "UID=root;" +
                 "PWD=password;";

            Console.WriteLine("Connecting to database...");
            MySqlConnection conn = new MySqlConnection(connStr);

            conn.Open();
            Console.WriteLine("Database: " + conn.Database);
            Console.WriteLine("Datasource: " + conn.DataSource);
            Console.WriteLine("Site: " + conn.Site);
            Console.WriteLine("Compression: " + conn.UseCompression);

            List<XmlNode> rows = GetJumpsXMLFromEVEAPI(ref data_time);
            CreateTable(ref conn, data_time);
            InsertData(ref conn, ref rows, data_time);
            //SelectQuery(ref conn);
            conn.Close();
            Console.WriteLine("Data insertion complete. Next pull at {0}",
                _cachedUntilUTC.ToLocalTime().AddSeconds(_seconds_of_delay).ToLongTimeString());
        }

        /* 
         * Makes a synchronous call to the api.eveonline.com server in order to
         * get the Jumps.xml file. The XML file is then parsed to retrieve
         * each of the <row> nodes that contain precious solarsystemID and shipJumps
         * data. This data is then returned back to main.
         */
        private static List<XmlNode> GetJumpsXMLFromEVEAPI(ref string data_time)
        {
            Console.WriteLine("Starting download...");
            List<XmlNode> rows = new List<XmlNode>();
            WebClient web = new WebClient();
            XmlDocument xmldoc = new XmlDocument();
            web.Proxy = null;
            string raw_dataTime = null;

            try
            {
                String response = web.DownloadString("https://api.eveonline.com/map/Jumps.xml.aspx");
                Console.WriteLine("Download complete...");
                xmldoc.LoadXml(response);
                xmldoc.Save("jumps.xml");
                raw_dataTime = xmldoc.SelectSingleNode("/eveapi/result/dataTime").InnerText;
                data_time = DateTime.Parse(raw_dataTime).ToString("yyyyMMdd_HHmmss");
                _documentTimeUTC = DateTime.Parse(xmldoc.SelectSingleNode("/eveapi/currentTime").InnerText);
                _cachedUntilUTC = DateTime.Parse(xmldoc.SelectSingleNode("/eveapi/cachedUntil").InnerText);
                _timer.Interval = _cachedUntilUTC.Subtract(_documentTimeUTC).TotalMilliseconds + (_seconds_of_delay * 1000);
                _timer.Enabled = true;

                foreach (XmlNode item in xmldoc.SelectNodes("/eveapi/result/rowset/row"))
                {
                    rows.Add(item);
                }
                Console.WriteLine("Row parsing complete...");
            }
            catch (Exception ex)
            {
                StreamWriter writer = new StreamWriter("api_or_parsing_error.log");
                writer.Write(ex.Message);
                writer.Close();
            }

            return rows;
        }

        /* 
         * Programmatically tells the MySQL server to create a 'systemjumps'
         * table if it doesnt exist already. The first column is the primary key
         * and holds each system's unique ID number. The second column is labeled
         * by the date and time the document was retrieved and stores the number
         * of jumps per system.
         */
        private static void CreateTable(ref MySqlConnection conn, string data_time)
        {
            Console.WriteLine("Creating table...");
            try
            {
                // check to see if system jumps table exists
                StringBuilder sql_query = new StringBuilder();
                sql_query.Append("CREATE TABLE IF NOT EXISTS systemjumps(solarSystemID int(8) PRIMARY KEY);");
                MySqlCommand cmd = new MySqlCommand(sql_query.ToString(), conn);
                cmd.ExecuteNonQuery();   
            }
            catch (Exception ex)
            {
                using (StreamWriter writer = new StreamWriter("create_table_error.log"))
                {
                    writer.Write(ex.Message);
                    writer.Close();
                }
            }         
        }

        /* 
         * Creates a new column using document retrieval time as the label.
         * Populates each row with the number of ships jumped into that system
         * since the last cache period. Solar systems not in the DB are given
         * new records and columns updated accordingly. Solar systems already
         * in the database have their appropriate columns updated with new data.
         */
        private static void InsertData(ref MySqlConnection conn, ref List<XmlNode> rows, string data_time)
        {
            Console.WriteLine("Inserting data...");
            StringBuilder sql_query = new StringBuilder();
            MySqlCommand cmd = null;
            string systemID = null;
            string shipJumps = null;

            try
            {
                // creating new column with dataTime as label
                sql_query.Append("ALTER TABLE systemjumps ADD ").Append(data_time).Append(" int(6) DEFAULT 0;");
                cmd = new MySqlCommand(sql_query.ToString(), conn);
                cmd.ExecuteNonQuery();
                sql_query.Clear();
            }
            catch(Exception ex)
            {
                StreamWriter writer = new StreamWriter("alter_table_error.log");
                writer.Write(ex.Message);
                writer.Close();
            }
            sql_query.Clear();

            try // populate solarsystemID row with new value, if necessary
            {
                foreach (XmlNode n in rows)
                {
                    systemID = n.Attributes[0].Value;
                    shipJumps = n.Attributes[1].Value;

                    sql_query.Append("INSERT INTO systemjumps (solarSystemID, ")
                        .Append(data_time).Append(") VALUES(").Append(systemID)
                        .Append(", ").Append(shipJumps).Append(") ON DUPLICATE KEY UPDATE ")
                        .Append(data_time).Append(" = ").Append(shipJumps).Append(";");
                    cmd = new MySqlCommand(sql_query.ToString(), conn);
                    cmd.ExecuteNonQuery();
                    sql_query.Clear();
                }
            }
            catch (Exception ex)
            {
                StreamWriter writer = new StreamWriter("populate_table_error.log");
                writer.Write(ex.Message);
                writer.Close();
            }
            sql_query.Clear();
        }

        /* 
         * Regurgitates the database into the console window. Not tested heavily,
         * but it does seem with work with 3 columns of data.
         */
        private static void SelectQuery(ref MySqlConnection conn)
        {
            Console.WriteLine("Running example query...");
            StringBuilder sql_query = new StringBuilder();
            sql_query.Append("select * from evesde.systemjumps;");
            MySqlCommand cmd = new MySqlCommand(sql_query.ToString(), conn);

            MySqlDataAdapter MyAdapter = new MySqlDataAdapter();
            MyAdapter.SelectCommand = cmd;
            DataTable dTable = new DataTable();
            MyAdapter.Fill(dTable);
            
            // displays column headers
            foreach (DataColumn c in dTable.Columns)
            {
                Console.Write(c.ColumnName + "\t");
            }
            Console.WriteLine();


            // displays column data
            DataRow[] currentRows = dTable.Select(null, null, DataViewRowState.CurrentRows);

            foreach (DataRow d in currentRows)
            {
                foreach (DataColumn column in dTable.Columns)
                {
                    Console.Write("{0}\t", d[column]);
                }

                Console.WriteLine();
            }
        }
    }
}
