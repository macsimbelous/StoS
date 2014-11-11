using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SQLite;
using System.Data;
using System.Globalization;
using System.Data.SqlServerCe;

namespace StoS
{
    class Program
    {
        static public SqlCeConnection sqlce_conn;
        static public SqlCeTransaction sqlce_trans;
        //static public SQLiteConnection connection;
        //static public SQLiteTransaction transaction;
        static int count_dub;
        static void Main(string[] args)
        {
            List<CImage> img_list = new List<CImage>();
            count_dub = 0;
            using (SQLiteConnection connection = new SQLiteConnection("data source=\"C:\\Users\\macs\\Dropbox\\utils\\Erza\\erza.sqlite\""))
            {
                connection.Open();
                using (SQLiteCommand command = new SQLiteCommand())
                {

                    command.CommandText = "select * from hash_tags";
                    command.Connection = connection;
                    SQLiteDataReader reader = command.ExecuteReader();
                    int count = 0;
                    while (reader.Read())
                    {
                        CImage img = new CImage();
                        img.hash = (byte[])reader["hash"];
                        if (img.hash.Length > 16)
                        {
                            string s = Encoding.ASCII.GetString(img.hash);
                            //Console.WriteLine(s);
                            s = s.Replace("X'",String.Empty);
                            s = s.Remove(s.Length-1);
                            //Console.WriteLine(s);
                            img.hash = HexStringToBytes(s);
                        }
                        img.is_deleted = (bool)reader["is_deleted"];
                        img.is_new = (bool)reader["is_new"];
                        img.id = (long)reader["id"];
                        if (!System.Convert.IsDBNull(reader["tags"]))
                        {
                            string[] t = ((string)reader["tags"]).Split(' ');
                            for (int i = 0; i < t.Length; i++)
                            {
                                if (t[i].Length > 0)
                                {
                                    img.tags.Add(t[i]);
                                    //if (all_tags.IndexOf(t[i]) < 0)
                                    //{
                                    //    all_tags.Add(t[i]);
                                    //}
                                }
                            }
                        }
                        /*if (!Convert.IsDBNull(reader["file_name"]))
                        {
                            img.file = (string)reader["file_name"];
                        }*/
                        img_list.Add(img);
                        count++;
                        Console.Write("\r" + count.ToString("#######"));
                    }
                    reader.Close();
                    Console.WriteLine("\rВсего: " + (count++).ToString());
                }

                sqlite_add(img_list);
                //sqlce_add(img_list);
                Console.ReadKey();
            }
        }
        static void sqlce_add(List<CImage> img_list)
        {
            count_dub = 0;
            Console.WriteLine("Добавляем хэши в базу данных SqlCe");
            DateTime start = DateTime.Now;
            sqlce_conn = new SqlCeConnection("Data Source='Erza.sdf';");
            sqlce_conn.Open();
            sqlce_trans = sqlce_conn.BeginTransaction();
            for (int i2 = 0; i2 < img_list.Count; i2++)
            {
                Console.Write("Обрабатываю хэш {0} ({1}/{2}) Число тегов: {3}\r", img_list[i2].hash_str, (i2 + 1), img_list.Count, img_list[i2].tags.Count);
                StringBuilder sb = new StringBuilder();
                for (int i4 = 0; i4 < img_list[i2].tags.Count; i4++)
                {
                    if (i4 > 0)
                    {
                        sb.Append(' ');
                    }
                    sb.Append(img_list[i2].tags[i4]);
                }
                using (SqlCeCommand insert_command = new SqlCeCommand("insert into hash_tags (hash, tags, is_new, is_deleted) values (@hash, @tags, @is_new, @is_deleted)", sqlce_conn))
                {
                    //insert_command.CommandText = "insert into hash_tags (hash, tags, is_new, is_deleted, file_name) values (@hash, @tags, @is_new, @is_deleted, @file_name)";
                    insert_command.Parameters.AddWithValue("hash", img_list[i2].hash);
                    insert_command.Parameters.AddWithValue("tags", sb.ToString());
                    insert_command.Parameters.AddWithValue("is_new", true);
                    insert_command.Parameters.AddWithValue("is_deleted", false);
                    //insert_command.Parameters.AddWithValue("file_name", null);
                    insert_command.ExecuteNonQuery();
                }
            }
            sqlce_trans.Commit();
            sqlce_conn.Close();
            DateTime finish = DateTime.Now;
            //Console.WriteLine(finish.ToString());
            Console.WriteLine("\nХэшей добавлено: " + img_list.Count.ToString() + " за: " + (finish - start).TotalSeconds.ToString("0.00") + " секунд (" + (img_list.Count / (finish - start).TotalSeconds) + " в секунду)");
            Console.WriteLine("Дубликатов: {0}", count_dub);
        }
        static void sqlite_add(List<CImage> img_list)
        {
            Console.WriteLine("Добавляем хэши в базу данных SQLite");
            using (SQLiteConnection connection = new SQLiteConnection("data source=\"C:\\Users\\macs\\Dropbox\\utils\\Erza\\erza-new.sqlite\""))
            {
                DateTime start = DateTime.Now;
                connection.Open();
                SQLiteTransaction transact = connection.BeginTransaction();
                for (int i2 = 0; i2 < img_list.Count; i2++)
                {
                    Console.Write("Обрабатываю хэш {0} ({1}/{2}) Число тегов: {3}\r", img_list[i2].hash_str, (i2 + 1), img_list.Count, img_list[i2].tags.Count);
                    using (SQLiteCommand command = new SQLiteCommand())
                    {
                        command.CommandText = "select * from hash_tags where hash = @hash";
                        //command.Parameters.Add("hash", DbType.Binary, 16).Value = il[i2].hash;
                        command.Parameters.AddWithValue("hash", img_list[i2].hash);
                        command.Connection = connection;
                        SQLiteDataReader reader = command.ExecuteReader();
                        if (reader.Read())
                        {
                            count_dub++;
                            string tags = System.Convert.ToString(reader["tags"]);
                            long id = System.Convert.ToInt64(reader["id"]);
                            bool is_deleted = System.Convert.ToBoolean(reader["is_deleted"]);
                            bool is_new = System.Convert.ToBoolean(reader["is_new"]);
                            reader.Close();
                            List<string> t = new List<string>(tags.Split(' '));
                            for (int i3 = 0; i3 < img_list[i2].tags.Count; i3++)
                            {
                                if (t.IndexOf(img_list[i2].tags[i3]) < 0)
                                {
                                    t.Add(img_list[i2].tags[i3]);
                                }
                            }
                            List<string> t2 = new List<string>();
                            for (int d = 0; d < t.Count; d++)
                            {
                                if (t[d].Length > 0) { t2.Add(t[d]); }
                            }
                            //tags = String.Empty;
                            StringBuilder sb = new StringBuilder();
                            for (int i4 = 0; i4 < t2.Count; i4++)
                            {
                                if (i4 > 0)
                                {
                                    //tags = tags + " ";
                                    sb.Append(' ');
                                }
                                //tags = tags + t2[i4];
                                sb.Append(t2[i4]);
                            }
                            tags = sb.ToString();
                            using (SQLiteCommand update_command = new SQLiteCommand(connection))
                            {
                                update_command.CommandText = "UPDATE hash_tags SET tags = @tags, is_new = @is_new, is_deleted = @is_deleted WHERE id = @id";
                                //update_command.CommandText = "UPDATE hash_tags SET tags = @tags WHERE hash = @hash";
                                //update_command.Parameters.Add("hash", DbType.Binary, 16).Value = il[i2].hash;
                                update_command.Parameters.AddWithValue("is_new", is_new);
                                update_command.Parameters.AddWithValue("is_deleted", is_deleted);
                                update_command.Parameters.AddWithValue("id", id);
                                update_command.Parameters.AddWithValue("tags", tags);

                                update_command.ExecuteNonQuery();
                            }
                        }
                        else
                        {
                            using (SQLiteCommand insert_command = new SQLiteCommand(connection))
                            {
                                insert_command.CommandText = "insert into hash_tags (hash, tags, is_new, is_deleted, file_name) values (@hash, @tags, @is_new, @is_deleted, @file_name)";
                                insert_command.Parameters.AddWithValue("hash", img_list[i2].hash);
                                insert_command.Parameters.AddWithValue("tags", img_list[i2].tags_string);
                                insert_command.Parameters.AddWithValue("is_new", true);
                                insert_command.Parameters.AddWithValue("is_deleted", false);
                                insert_command.Parameters.AddWithValue("file_name", null);
                                insert_command.ExecuteNonQuery();
                            }
                        }
                    }
                    /*StringBuilder sb = new StringBuilder();
                    for (int i4 = 0; i4 < img_list[i2].tags.Count; i4++)
                    {
                        if (i4 > 0)
                        {
                            sb.Append(' ');
                        }
                        sb.Append(img_list[i2].tags[i4]);
                    }
                    using (SQLiteCommand insert_command = new SQLiteCommand(connection))
                    {
                        insert_command.CommandText = "insert into hash_tags (hash, tags, is_new, is_deleted, file_name) values (@hash, @tags, @is_new, @is_deleted, @file_name)";
                        insert_command.Parameters.AddWithValue("hash", img_list[i2].hash);
                        insert_command.Parameters.AddWithValue("tags", sb.ToString());
                        insert_command.Parameters.AddWithValue("is_new", true);
                        insert_command.Parameters.AddWithValue("is_deleted", false);
                        insert_command.Parameters.AddWithValue("file_name", null);
                        insert_command.ExecuteNonQuery();
                    }*/
                }
                transact.Commit();
                DateTime finish = DateTime.Now;
                //Console.WriteLine(finish.ToString());
                Console.WriteLine("\nХэшей добавлено: " + img_list.Count.ToString() + " за: " + (finish - start).TotalSeconds.ToString("0.00") + " секунд (" + (img_list.Count / (finish - start).TotalSeconds) + " в секунду)");
                Console.WriteLine("Дубликатов: {0}", count_dub);}
        }
        static byte[] HexStringToBytes(string hexString)
        {
            if (hexString == null)
            {
                throw new ArgumentNullException("hexString");
            }

            if ((hexString.Length & 1) != 0)
            {
                throw new ArgumentOutOfRangeException("hexString", hexString, "hexString must contain an even number of characters.");
            }

            byte[] result = new byte[hexString.Length / 2];

            for (int i = 0; i < hexString.Length; i += 2)
            {
                result[i / 2] = byte.Parse(hexString.Substring(i, 2), NumberStyles.HexNumber);
            }

            return result;
        }
    }
    public class CImage
    {
        public long image_id;
        public long file_id;
        public bool is_new = true;
        public bool is_deleted = false;
        public long id;
        public byte[] hash;
        public string file = null;
        public List<string> tags = new List<string>();
        public string hash_str;
        public string tags_string
        {
            get
            {
                string s = String.Empty;
                for (int i = 0; i < tags.Count; i++)
                {
                    if (i > 0)
                    {
                        s = s + " ";
                    }
                    s = s + tags[i];
                }
                return s;
            }
            set
            {
                string[] t = value.Split(' ');
                for (int i = 0; i < t.Length; i++)
                {
                    if (t[i].Length > 0)
                    {
                        tags.Add(t[i]);
                    }
                }
            }
        }
        public override string ToString()
        {
            if (this.file != String.Empty)
            {
                return file.Substring(file.LastIndexOf('\\') + 1);
            }
            else
            {
                return "No File!";
            }
        }
    }
}
